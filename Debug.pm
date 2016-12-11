package dmearns::Debug;

use POSIX qw(strftime);
use Carp;
use strict;
use warnings;

use base qw(Exporter);
our @EXPORT_OK = qw( report_debug debug set_debug_level get_time get_time_str time_diff rotate_logfile check_logfile_rotation );
our $VERSION = '1.00';
our $logfile;
our $log_iterations = 10;
our $log_maxsize = 10_000_000;
our $log_initialized = 0;
our $debug = 100;
our $hires = 0;

eval { require Time::HiRes };
unless ($@) {
  import Time::HiRes qw( gettimeofday tv_interval );
  $hires = 1;
}

sub get_time
{
  if ($hires) {
    my ( $seconds, $microseconds ) = gettimeofday();
    return [ $seconds, $microseconds ];
  } else {
    my $seconds = time();
    return $seconds;
  }
}

sub get_time_str
{
  my ($arg) = @_;
  my $now = "";
  if ($hires) {
    my ($seconds, $microseconds) = @{$arg};
    $now = strftime( '%b %d %Y %H:%M:%S', localtime($seconds) );
    $now .= sprintf( ".%03d", int( $microseconds / 1000 ) );
  } else {
    $now = strftime( '%b %d %Y %H:%M:%S', localtime( $arg ) );
  }
  return $now
}

sub time_diff
{
  my ($t1, $t2) = @_;
  if ($hires) {
    return sprintf( "%10.3f", tv_interval( $t1, $t2 ));
  } else {
    my $dsec = $t2 - $t1;
    return sprintf( "%d", $dsec );
  }
}

# Rotate logfiles
#-------------------------------------------------------------------------------
sub rotate_logfile
{
  my ($i, $file1, $file2);

  return if !defined( $logfile );

# delete files, counting up - in case iterations has been reduced

  for ($i = $log_iterations; ; $i++) {
    $file1 = "$logfile.$i";
    if ( -f $file1 ) {
      unlink $file1;
    } else {
      last;
    }
  }

# rename files counting down

  $i = $log_iterations;
  while ( $i > 1 ) {
    $file1 = "$logfile.$i";
    $i--;
    $file2 = "$logfile.$i";
    if ( -f $file2 && !rename( $file2, $file1 ) ) {
      my $msg = "cannot rename $file2, $file1 - $!";
# can't call report_debug - infinite recursion!
#      report_debug( 0, $msg );
      croak $msg;
    }
  }

# handle last case

  $file1 = "$logfile.$i";
  $file2 = "$logfile";

  if ( -f $file2 && !rename( $file2, $file1 ) ) {
    my $msg = "cannot rename $file2, $file1 - $!";
# can't call report_debug - infinite recursion!
#    report_debug( 0, $msg );
    croak $msg;
  }

# finally, open the log file

  open( STDOUT, ">>", $file2 ) || croak "cannot open $file2";
  open( STDERR, ">>", $file2 ) || croak "cannot open $file2";

  return 0;
}

sub check_logfile_rotation
{
  if ( !$log_initialized ) {
    open( STDOUT, ">>", $logfile ) || croak "cannot open $logfile";
    open( STDERR, ">>", $logfile ) || croak "cannot open $logfile";
    $log_initialized = 1;
  }
  if ( defined($log_maxsize) && tell(STDOUT) > $log_maxsize ) {
    my $now = strftime( '%b %d %Y %H:%M:%S', localtime( time ) );
    print "[$now] ----- automatic logfile rotation ----- \n";
    rotate_logfile();
    print "[$now] ----- automatic logfile rotation ----- \n";
  }
  return 0;
}

sub report_debug
{
  my( $level, $message ) = @_;

  return if $debug < $level;

  my $t = get_time();
  my $now = get_time_str( $t );
  my ($package, $filename, $line) = caller(0);
  print "[$now]{$level}$package:$line $message\n";
  return 0;
}
*debug = &report_debug;

sub set_debug_level
{
  my( $level) = @_;

  $debug = $level;
  return 0;
}

1;
