#!/usr/bin/perl
use strict;
use warnings;
use Data::Dumper;

my %results;
my @files = @ARGV;

for my $f (@files) {
   open (F, $f);
   my @lines = <F>;
   close(F);

   my $last_bench;
   for my $l (@lines) {
      if($l =~ m/(YCSB \w|Prod).*(Zipf|Uniform|Production\d).*\((\d+) (req|scan)/) {
         $last_bench = "$2-$1";
         $results{$last_bench}->{throughput}->{$f} = $3;
      } elsif($l =~ m/AVG - (\d+)/) {
         $results{$last_bench}->{AVG}->{$f} = $1;
      } elsif($l =~ m/99p - (\d+)/) {
         $results{$last_bench}->{'99p'}->{$f} = $1;
      } elsif($l =~ m/max - (\d+)/) {
         $results{$last_bench}->{max}->{$f} = $1;
      }
   }
}

for my $bench (sort keys %results) {
   my @keys = sort { $results{$bench}->{throughput}->{$a} <=> $results{$bench}->{throughput}->{$b} } keys %{$results{$bench}->{throughput}};

   print "$bench:\n";
   print "File\t".join("\t", @keys)."\n";

   for my $k ('throughput', 'AVG', '99p', 'max') {
      print "$k\t".join("\t", map { $results{$bench}->{$k}->{$_} } @keys)."\n";
   }

   print "\n";
}

#print Dumper(\%results);
