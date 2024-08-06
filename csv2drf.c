/*
 * This tool converts a csv to 
 *
 * Compile command:
 *  gcc -o drfwrite examplewrite.c -I/usr/local/include/digital_rf/ -I/usr/include/hdf5/serial -ldigital_rf -L/usr/local/lib/arm-linux-gnueabihf/ -lhdf5_serial
 * 
 * TODO: Uncomment zero adjust code block in prod
 */
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include "digital_rf.h"

#define SIGNAL_LENGTH 8000

uint64_t time_counter(void) // Author: Bill Blackwell
{
    struct timespec now;

    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

unsigned int i_samples[SIGNAL_LENGTH][3];
FILE *fpSamp = NULL;
int nframes = 0;

// Get one data block
int getBlock(void)
{
    char sbuf[100], ts[30], *pc;

    // Read one block of samples
    int nsamp = 0;
    int ts_found = 0;
    int cs_found = 0;
    while (fgets(sbuf, sizeof(sbuf) - 1, fpSamp) != NULL)
    {
        // Flag when timestamp and checksum are found
        switch (sbuf[0])
        {
        case 'T':
            ts_found = 1; // timestamp found
            pc = strchr(sbuf, '\n');
            if (pc)
                *pc = '\0';
            strncpy(ts, sbuf, sizeof(ts) - 1);
            continue;
        case 'C':
            cs_found = 1; // checksum found
            break;
        default:
            break;
        }

        if (ts_found)
        {
            // Found beginning of sample frame
            // startTime = time_counter();
            if (cs_found)
            {
                // Found end of sample frame
                if (nsamp == SIGNAL_LENGTH)
                {
                    // printf("%s nsamp = %d     Beacon1      Beacon2      Beacon3\n", ts, nsamp);
                    printf("%s\n", ts);
                }
                else
                {
                    printf("Error: %s nsamp = %d, skipped\n", ts, nsamp);
                }

                nframes++;
                break; // out of while fgets loop
            }
            else
            {
                // Convert samples to integer
                sscanf(sbuf, "%04x,%04x,%04x\n", &i_samples[nsamp][0], &i_samples[nsamp][1], &i_samples[nsamp][2]);

                // Zero adjust using node 33's zeros.dat values.
                // TODO: read this in from the file.
                // i_samples[nsamp][0] += (0x8000 - 0x7f0d);
                // i_samples[nsamp][1] += (0x8000 - 0x7f0c);
                // i_samples[nsamp][2] += (0x8000 - 0x7f3c);

                nsamp++;
            }
        }
        else
            printf(sbuf);
    }

    return nsamp;
}

int main(int argc, char *argv[])
{
    /* local variables */
    Digital_rf_write_object *data_object = NULL; /* main object created by init */
    uint64_t vector_leading_edge_index = 0;      /* index of the sample being written starting at zero with the first sample recorded */
    uint64_t global_start_index;                 /* start sample (unix time * sample_rate) of first measurement - set below */
    int i, result;

    /* writing parameters */
    uint64_t sample_rate_numerator = SIGNAL_LENGTH; /* 8000 Hz sample rate - typically MUCH faster */
    uint64_t sample_rate_denominator = 1;
    uint64_t subdir_cadence = 3600; /* Number of seconds per subdirectory */
    uint64_t millseconds_per_file = 60000;
    int compression_level = 9;
    int checksum = 0;
    int is_complex = 0;
    int is_continuous = 1; /* continuous data written */
    int num_subchannels = 3;
    int marching_periods = 0; /* no marching periods when writing */
    char uuid[100] = "Fake UUID - use a better one!";
    uint64_t vector_length = SIGNAL_LENGTH; /* number of samples written for each call - typically MUCH longer */

    /* start recording at global_start_sample */
    // TODO: read the start time from the first comment in the file
    global_start_index = (uint64_t)(1708653600 * (long double)sample_rate_numerator / sample_rate_denominator);

    printf("Writing data to multiple files and subdirectores in hdf5 channel ch0\n");
    // result = system("rm -rf hdf5 ; mkdir hdf5 ; mkdir hdf5/ch0");
    result = system("rm -rf test_hdf5 ; mkdir test_hdf5 ; mkdir test_hdf5/ch0");

    /* init hdf5 write object */
    data_object = digital_rf_create_write_hdf5("test_hdf5/ch0", H5T_NATIVE_UINT, subdir_cadence, millseconds_per_file,
                                               global_start_index, sample_rate_numerator, sample_rate_denominator, uuid, compression_level, checksum, is_complex, num_subchannels,
                                               is_continuous, marching_periods);
    if (!data_object)
        exit(-1);

    if (argc > 1)
    {
        // Open sample file
        fpSamp = fopen(argv[1], "r");
        if (fpSamp == NULL)
        {
            perror("Sample file");
            exit(EXIT_FAILURE);
        }
    }
    else
        fpSamp = stdin;

    i = 0;
    while (getBlock() == SIGNAL_LENGTH)
    {
        /* write continuous data */
        // TODO: get offset from timestamp 
        result = digital_rf_write_hdf5(data_object, vector_leading_edge_index + i * 8000, i_samples, vector_length);
        if (result)
            exit(-1);
        i++;
    }

    printf("%d frames processed\n", nframes);

    /* close */
    digital_rf_close_write_hdf5(data_object);
    fclose(fpSamp);

    printf("done - examine hdf5/ch0 for data\n");
    return (0);
}
