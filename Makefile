all:
	gcc -g -o cluster-cli crc16.c hicluster.c example.c -lhiredis -I "./" -L "./"
