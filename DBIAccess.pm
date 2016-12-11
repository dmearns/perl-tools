package dmearns::DBIAccess;

use strict;
use warnings;
use DBI;

use vars qw(@EXPORT @ISA $VERSION);
require Exporter;
@ISA = qw( Exporter );
@EXPORT = qw( run_sql );

$VERSION = '1.00';

sub new
{
  my $class = shift;
  my %args = @_;

  my $self = {};
  bless $self, $class;

  $self->{dsn} = $args{dsn};
  $self->{username} = $args{username};
  $self->{password} = $args{password};
  $self->{attr} = $args{attr};
  $self->{connected} = 0;
  $self->{errstr} = "";
  return $self;
}

sub connect
{
  my $self = shift;

  $self->{conn_handle} = DBI->connect($self->{dsn}, $self->{username}, $self->{password}, $self->{attr});
  $self->{errstr} = $DBI::errstr;
  $self->{connected} = 1 if defined $self->{conn_handle};
  return $DBI::err;
}

sub disconnect
{
  my $self = shift;

  $self->{connected} = 0;
  $self->{conn_handle}->disconnect();
  $self->{errstr} = $DBI::errstr;
  return $DBI::err;
}

sub from_file
{
  my $self = shift;
  my %args = @_;

  my $n = 0;
  if (open(my $fh, "<", $args{filename})) {
    foreach (<$fh>) {
      s/#.*//;
      if (/([^=]+)\s*=\s*(.*)/) {
        $self->{$1} = $2;
        $n++;
      }
    }
    close $fh;
    return $n;
  } else {
    return 0;
  }
}

sub prepare
{
  my ($self,$sql) = @_;

  $self->{statement} = undef;
  if (!$self->{connected}) {
    $self->connect();
    return () if !$self->{connected};
  }

  my $sth = $self->{conn_handle}->prepare($sql, $self->{attr});
  $self->{errstr} = $DBI::errstr;
  return $self->{conn_handle}->err unless $sth;
  $self->{statement} = $sth;
  return 1;
}

sub do_prepared
{
  my ($self,$hash_flag,$bind_values) = @_;

  $hash_flag = 0 unless $hash_flag;
  $bind_values = [] if !defined $bind_values;

  my $rc = $self->{statement}->execute( @{$bind_values} );
  $self->{errstr} = $DBI::errstr;
  return $self->{conn_handle}->err unless $rc;

  my $aref = [];
  if ($self->{statement}->{NUM_OF_FIELDS}) {
    $aref = $hash_flag 
      ? $self->{statement}->fetchall_arrayref({}) 
      : $self->{statement}->fetchall_arrayref();
    $self->{errstr} = $DBI::errstr;
  }
  return wantarray ? @{$aref} : $aref;
}

sub sql
{
  my ($self,$sql,$proc,$hash_flag,$bind_values) = @_;

  $hash_flag = 0 unless $hash_flag;
  $bind_values = [] if !defined $bind_values;

  die "proc not implemented" if defined $proc;

  if (!$self->{connected}) {
    $self->connect();
    return () if !$self->{connected};
  }

  my $sth = $self->{conn_handle}->prepare($sql, $self->{attr});
  $self->{errstr} = $DBI::errstr;
  return $self->{conn_handle}->err unless $sth;

  my $rc = $sth->execute( @{$bind_values} );
  $self->{errstr} = $DBI::errstr;
  return $self->{conn_handle}->err unless $rc;

  my $aref = [];
  if ($sth->{NUM_OF_FIELDS}) {
    $aref = $hash_flag ? $sth->fetchall_arrayref({}) : $sth->fetchall_arrayref();
    $self->{errstr} = $DBI::errstr;
  }
  return wantarray ? @{$aref} : $aref;
}

1;
