package dmearns::SybAccess;
use Sybase::DBlib;
use Data::Dumper;
use strict;

use vars qw(@EXPORT @ISA $VERSION);
require Exporter;

@ISA = qw( Exporter );
@EXPORT = qw( run_sql );

$VERSION = '1.00';

=head1 NAME

dmearns::SybAccess - Wrapper for Sybase::DBlib.

=head1 SYNOPSIS

  use dmearns::SybAccess;
  dbh = new dmearns::SybAccess( 'Database'=>'dmearns' );
  @rows = $dbh->sql( "select ...", undef, 1 );
 
  For legacy scripts, first three args can be treated the same as 
  Sybase::DBlib, subsequent args must be keyword/value

  $dbh = new dmearns::SybAccess( 'groKKer', 'grokKer', 'SYBASE' );

  The two methods can be combined.
  
  $dbh = new dmearns::SybAccess( 'groKKer', 'grokKer', 'SYBASE',
    'message_handler' => 'html_message_handler' 
    'deadlock_retry_count' => 10 );

=head1 DESCRIPTION

Module to wrap Sybase::DBlib, providing deadlock handling, message 
handling, database username/password discovery, in addition to easy 
transition for legacy code.

=cut

my $deadlock;

# private method to do lookup in grokdef file

sub grokdef_lookup
{
  my $self = shift;
  my $grok = $ENV{'GROK'} || "/usr/grok";
  my ( $section, %user, %pass );

  local( *FILE );
  open( FILE, "<$grok/def/grokdef" ) || return undef;
  while (<FILE>) {
    next if /^#/;
    $section = $1 if /\[(.*LOGIN)\]/;
    $user{ $section } = $1 if /^\s*USERNAME\s+(.*)/;
    $pass{ $section } = $1 if /^\s*PASSWORD\s+(.*)/;
  }
  close( FILE );
  if ( $self->{'Database'} eq 'grok_db' ) {
    return ($user{ 'LOGIN' }, $pass{ 'LOGIN' } );
  }
  if ( $self->{'Database'} eq 'grokras_db' ) {
    return ($user{ 'RASLOGIN' }, $pass{ 'RASLOGIN' } );
  }
  return undef;
}

# private method to determine login info by
# discovery method

sub loginByMethod
{
  my ($self, $method) = @_;
  my ($user, $pw);

  if ( $method eq "usegrokdef" ) {
    ($user, $pw) = $self->grokdef_lookup();
  } elsif ( $method eq "useEnv" ) {
    if ( $self->{'Database'} eq "grok_db" ) {
      ($user, $pw) = ( $ENV{'GROKLOGIN'}, $ENV{'GROKPASSWORD'} );
    }
    if ( $self->{'Database'} eq "grokras_db" ) {
      ($user, $pw) = ( $ENV{'RASLOGIN'}, $ENV{'RASPASSWORD'} );
    }
  } elsif ( $method eq "useDefault" ) {
    if ( $self->{'Database'} eq "grok_db" ) {
      ($user, $pw) = ( "groKKer", "groKKer" );
    }
    if ( $self->{'Database'} eq "grokras_db" ) {
      ($user, $pw) = ( "ras", "groKKer" );
    }
  } else {
    return undef;
  }
  return ($user, $pw );
}

=head1 METHODS

=over

=item new

Object constructor.  Optional arguments may be passed as 
keyword => value pairs.  Valid keywords are:

    Server Username Password Database discovery message_handler
deadlock_retry_count

If the first argument is not one of these keywords, then the first
three arguments are considered to be Username, Password and Server,
so that it then works just like the Sybase::DBlib new.

=cut

sub new($)
{
  my ( $class, @args ) = @_;
  my $self = { 
    'Server' => $ENV{'DSQUERY'} || 'SYBASE', 
    'Username' => 'groKKer', 
    'Password' => 'groKKer', 
    'Database' => 'grok_db', 
    'message_handler' => 'std_message_handler',
    'deadlock_retry_count' => 5,
    'deadlock_flag' => 0,
  };
  $self->{'discovery'} = [ qw( useEnv usegrokdef useDefault ) ];
  my ($k, $err, $i );
  my @allowed = qw( Server Username Password Database discovery message_handler 
    deadlock_retry_count );

  bless $self, $class;

# is first argument in the allowed list?  if so, then all should be
# if not, then the first 3 args are legacy (username, passwrod, server)

  if ( scalar( @args ) > 0 ) {
    if ( grep( /^$args[0]$/, @allowed ) ) {
      $i = 0;
    } else {
      $i = 3;
      $self->{'Username'} = $args[0];
      $self->{'Password'} = $args[1];
      $self->{'Server'}   = $args[2];
      $self->{'Database'} = "grok_db"    if $self->{'Username'} eq "groKKer";
      $self->{'Database'} = "grokras_db" if $self->{'Username'} eq "ras";
    }

    for ( ; $i <scalar(@args); $i++) {
      $k = $args[$i++];
      if ( grep( /^$k$/, @allowed ) ) {
        $$self{ $k } = $args[$i];
      } else {
        $err .= "illegal option: $k => $args[$i]\n";
      }
    }
  }

  die "$err    in ${class}::new()" if defined $err;

  $self->{'default_message_handler'} = &dbmsghandle( 'silent_message_handler' );
  $self->{'default_error_handler'}   = &dberrhandle( 'non_error_handler' );

  foreach my $disc_method ( @{$self->{'discovery'}} ) {
#    print "trying $disc_method\n";
    my ($user, $pass) = $self->loginByMethod( $disc_method );
    if ( defined( $user ) && defined( $pass ) ) {
      $self->{'handle'} = new Sybase::DBlib( $user, $pass, $self->{'Server'});
    }

    if ( defined( $self->{'handle'} ) ) {

      $self->{'old_message_handler'} 
        = &dbmsghandle( $self->{'message_handler'} ) 
        if defined( $self->{'message_handler'} );

      $self->{'old_error_handler'} 
        = &dberrhandle( $self->{'default_error_handler'} )
        if defined( $self->{'default_error_handler'} );

      last;
    }
  }

  if ( $err ) {
    die "error in ${class}::new()";
  } elsif( !defined( $self->{'handle'} )) {
    return undef;
  } else {
    return $self;
  }
}

=item get_handle

Return the Sybase::DBlib handle opened by the object.

=cut

sub get_handle
{
  my $self = shift;
  return $self->{'handle'};
}

# debugging

sub dump
{
  my $self = shift;

  $Data::Dumper::Indent = 1;
  print Data::Dumper->Dump( [$self],["self"] );

}

=item set_retry_count

Set the deadlock retry count

=cut

sub set_retry_count
{
  my $self = shift;
  my $count = shift;

  if ( defined( $count ) ) {
    $self->{'deadlock_retry_count'} = $count;
  }
}

=item get_retry_count

Return the deadlock retry count

=cut

sub get_retry_count
{
  my $self = shift;
  return $self->{'deadlock_retry_count'};
}

# error handler that is silent, needed to suppress messages when
# we are trying to find a login that works

sub non_error_handler { return INT_CANCEL; }

=item std_message_handler

Message Handler routine that formats messages for output to screen.

=cut

sub std_message_handler
{
   my ($db, $message, $state, $severity, $text, $server, $procedure, $line)
       = @_;
   my ($row);


# Don't display 'informational' messages:
  if ($severity > 0) {
    $deadlock++ if $message == 1205;
    print "\n";
    print "Sybase message $message, Severity $severity, state $state";
    print "\nServer `$server'" if defined ($server);
    print "\nProcedure `$procedure'" if defined ($procedure);
    print "\nLine $line" if defined ($line);
    print "\n    $text\n\n";

# &dbstrcpy returns the command buffer.

    if(defined($db)) {
      my ($lineno, $cmdbuff) = (1, undef);

      $cmdbuff = &Sybase::DBlib::dbstrcpy($db);

      foreach $row (split(/\n/, $cmdbuff)) {
        printf( "%5d> %s\n", $lineno++, $row );
      }
    }
    print "\n";
  } elsif ($message == 0) {
    print "$text\n";
  }

  0;
}

=item html_message_handler

Message Handler routine that formats messages as HTML.

=cut

sub html_message_handler
{
   my ($db, $message, $state, $severity, $text, $server, $procedure, $line)
       = @_;
   my ($row);

# Don't display 'informational' messages:
  if ($severity > 0) {
    $deadlock++ if $message == 1205;
    print "<pre>\n";
    print "Sybase message $message, Severity $severity, state $state";
    print "\nServer `$server'" if defined ($server);
    print "\nProcedure `$procedure'" if defined ($procedure);
    print "\nLine $line" if defined ($line);
    print "\n    $text\n\n";

# &dbstrcpy returns the command buffer.

    if(defined($db)) {
      my ($lineno, $cmdbuff) = (1, undef);

      $cmdbuff = &Sybase::DBlib::dbstrcpy($db);

      foreach $row (split(/\n/, $cmdbuff)) {
        printf( "%5d> %s\n", $lineno++, $row );
      }
    }
    print "</pre>\n";
  } elsif ($message == 0) {
    print "<pre>$text</pre>\n";
  }

  0;
}

=item silent_message_handler

Message Handler routine that suppresses error messages.

=cut

sub silent_message_handler
{
   my ($db, $message, $state, $severity, $text, $server, $procedure, $line)
       = @_;
   my ($row);

  if ($severity > 0) {
    $deadlock++ if $message == 1205;
  }

  1;
}

=item sql

Execute SQL.  Copies syntax of Sybase::DBlib routine, but provides
deadlock handling as long as the message handler flags deadlocks.

=cut

sub sql
{
  my $self = shift;
  my $sql = shift;
  my $proc = shift;
  my $hash_flag = shift;

  my $dbh = $self->{'handle'};
  my ($i, @rows);

  for ($i=0; $i < $self->{'deadlock_retry_count'}; $i++) {
    $deadlock = 0;
    @rows = $dbh->sql( $sql, $proc, $hash_flag );
    last if $deadlock == 0;
  }
  $self->{'deadlock_flag'} = $deadlock;
  if ( $deadlock ) {
#    print "deadlock retries exceeded<br>\n";
    return undef;
  } else {
    return wantarray ? @rows : \@rows;
  }

}

=item run_sql

This convenience function is exported to make porting of existing
code easier.

=cut

sub run_sql
{
  my ($dbh, @args) = @_;

  return $dbh->sql( @args );
}

=item deadlock_flag

The state of the deadlock flag may be accessed as $dbh->{'deadlock_flag'}.

=cut

1;
