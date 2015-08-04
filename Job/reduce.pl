#!/usr/bin/perl

$old_key = '';
$wban_max = '';
$key_max = '';
$max_hour = '';
while(<stdin>) {

	@chunks = split(';', $_);
	

	if (($old_key eq $chunks[0]) && ($wban_max eq $chunks[1])){
		if($chunks[2] > $key_max) {
			$key_max = $chunks[2];
			$max_hour = $chunks[3];
		}
	} else {
		if ($old_key ne '') {
			print "Fecha: ". $old_key . " - Wban:" . $wban_max . " - Temp Max: " . $key_max . " - Hora: " . $max_hour;
		}
		$old_key = $chunks[0];
		$wban_max = $chunks[1];
		$key_max = $chunks[2];		
		$max_hour = $chunks[3];
	}
}
