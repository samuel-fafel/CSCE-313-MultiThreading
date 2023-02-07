#include <fstream>
#include <iostream>
#include <thread>
#include <sys/time.h>
#include <sys/wait.h>

#include "BoundedBuffer.h"
#include "common.h"
#include "Histogram.h"
#include "HistogramCollection.h"
#include "FIFORequestChannel.h"

/*


./test-files/tester < test-files/test_single_msg.txt
./test-files/tester < test-files/test_push_synch.txt
./test-files/tester < test-files/test_pop_synch.txt
./test-files/tester < test-files/test_both_synch.txt
./client -n 1000 -p 5 -w 100 -h 20 -b 5
./client -n 10000 -p 10 -w 100 -h 20 -b 30
./client -w 100 -b 30 -f 1.csv; diff -sqwB BIMDC/1.csv received/1.csv
./client -w 100 -b 50 -f test.bin; diff -sqwB BIMDC/test.bin received/test.bin
./client -w 100 -b 50 -m 8192 -f test.bin; diff -sqwB BIMDC/test.bin received/test.bin
*/

// ecgno to use for datamsgs
#define ECGNO 1

bool DEBUG = false;
using namespace std;

struct bundle {
    int first = -1;
    double second = -1.0;
};
/**/
// patient number, number of requests, buffer size, buffer
void patient_thread_function (int p_no, int n, int m, BoundedBuffer* req_buf) { 
    if (DEBUG) cout << "CALLED PATIENT THREAD FUNCTION" << endl;
    // functionality of the patient threads
    for (double t = 0.000; t < n*0.004; t += 0.004) { // for n requests, produce a datamsg(P, time, ECGNO) and push to request buffer
        char* msg = new char[m]; // message size
        datamsg dm(p_no, t, ECGNO); // create data msg
        memcpy(msg, &dm, sizeof(datamsg)); // memcpy bits into msg
        req_buf->push(msg, sizeof(datamsg)); // push data msg to request buffer
        delete[] msg;
    }
    if (DEBUG) cout << "EXITING PATIENT THREAD FUNCTION" << endl;
}

// filename, buffer size, buffer, control channel
void file_thread_function (string filename, int m, BoundedBuffer* req_buf, FIFORequestChannel* control) { 
    // functionality of the file thread
    filemsg fsize(0, 0); // create filemsg

    int len = sizeof(filemsg) + (filename.size() + 1); // get file size
    char* fbuf = new char[len];
    memcpy(fbuf, &fsize, sizeof(filemsg));
    strcpy(fbuf + sizeof(filemsg), filename.c_str());
    control->cwrite(fbuf, len); // write in control channel

    int64_t filesize = 0;
    control->cread(&filesize, sizeof(int64_t)); // read filesize
    //cout << "File length is: " << filesize << " bytes" << endl;

    filename = "received/" + filename; // create output file in ( /received/ ) with write and binary
    /**/
    FILE* of;
    if (!(of = fopen(filename.c_str(), "w+b"))) {
        perror("Error Opening File");
    }
    /**/
    //cout << "Opening filestream to: " << filename << endl;
    fseek(of, filesize, SEEK_SET);
    fclose(of);
    //*/

    int datasegs = filesize/m;
    for (int s = 0; s <= datasegs; ++s) {
        filemsg* fm = (filemsg*) fbuf;
        int rem = filesize - s*m; // find remaining bits in file     
        fm->offset = s*m;
        fm->length = min(m, rem);
        req_buf->push(fbuf, len);
    }
    delete [] fbuf;
}

// message size, request buffer, response buffer, worker channel, 
void worker_thread_function (int m, BoundedBuffer* rqst_buf, BoundedBuffer* resp_buf, FIFORequestChannel* pipe, string filename) {
    if (DEBUG) cout << "CALLED WORKER THREAD FUNCTION" << endl;
    // functionality of the worker threads
    for (;;) { // forever loop
        char* rqst = new char[m];
        int len = rqst_buf->pop(rqst, m); // pop msg from server, determine type of msg    
        MESSAGE_TYPE type = *(MESSAGE_TYPE*)rqst;
        if (type == DATA_MSG) { // IF DATA:
            datamsg* d = (datamsg*) rqst;
            int pno = d->person;
            double resp;
            pipe->cwrite(rqst, m); // send request across a FIFO
            pipe->cread(&resp, sizeof(double)); // collect response
            bundle B; // create a pair of p_no and response
            B.first = pno;
            B.second = resp;

            char* buf = new char[sizeof(B)];
            memcpy(buf, &B, sizeof(B)); // convert pair to char* with memcpy
            resp_buf->push(buf, sizeof(B)); // push pair to reponse_buffer
            delete [] buf;
        }
        else if (type == FILE_MSG) { // IF FILE
            string FN = "received/"+filename;
            filemsg* f = (filemsg*) rqst;

            char* buf3 = new char[m];
            char* fbuf = new char[len];
            len = sizeof(filemsg) + (filename.size() + 1); // get file size
            memcpy(fbuf, f, sizeof(filemsg));
            strcpy(fbuf + sizeof(filemsg), filename.c_str());
            pipe->cwrite(rqst, len); // send request across a FIFO
            pipe->cread(buf3, f->length); // collect response

            FILE* of = fopen(FN.c_str(), "r+b"); // open file in update mode
            fseek(of, f->offset, SEEK_SET); // fseek to offset
            fwrite(buf3, sizeof(char), f->length, of); // write to buffer
            fclose(of);
            delete [] buf3;
        }
        delete [] rqst;
        if (type == UNKNOWN_MSG) break;
    }
    if (DEBUG) cout << "EXITING WORKER THREAD FUNCTION" << endl;
}

// buffer size, response buffer, histogram collection
void histogram_thread_function (int m, BoundedBuffer* resp_buf, HistogramCollection* HC) {
    if (DEBUG) cout << "CALLED HISTOGRAM THREAD FUNCTION" << endl;
    // functionality of the histogram threads
    for (;;) { // forever loop
        char* buf = new char[m];
        if (DEBUG) cout << "HTF: popping from resp_buf" << endl;
        resp_buf->pop(buf, m); // pop msg from buffer
        if (DEBUG) cout << "HTF: converting back to pair" << endl;
        bundle* P = (bundle*) buf; // convert back to pair
        if (DEBUG) cout << "HTF: " << P->first << ", " << P->second << endl;
        if (P->first == -1) {
            delete [] buf;
            if (DEBUG) cout << "HTF: TIME TO QUIT" << endl;
            break;
        }
        if (DEBUG) cout << "HTF: updating HC" << endl;
        HC->update(P->first, P->second); // update HC
        delete [] buf;
    }
    if (DEBUG) cout << "EXITING HISTOGRAM THREAD FUNCTION" << endl;
}
//*/

int main (int argc, char* argv[]) {
    int n = 1000;	// default number of requests per "patient"
    int p = 1;		// number of patients [1,15]
    int w = 1;	    // default number of worker threads
	int h = 1;		// default number of histogram threads
    int b = 20;		// default capacity of the request buffer (should be changed)
	int m = MAX_MESSAGE;	// default capacity of the message buffer
	string f = "";	// name of file to be transferred
    
    // read arguments
    int opt;
	while ((opt = getopt(argc, argv, "n:p:w:h:b:m:f:")) != -1) {
		switch (opt) {
			case 'n':
				n = atoi(optarg);
                break;
			case 'p':
				p = atoi(optarg);
                break;
			case 'w':
				w = atoi(optarg);
                break;
			case 'h':
				h = atoi(optarg);
				break;
			case 'b':
				b = atoi(optarg);
                break;
			case 'm':
				m = atoi(optarg);
                break;
			case 'f':
				f = optarg;
                break;
		}
	}
    
    if (n >= 10000) {
        n = 9999;
    }
	// fork and exec the server
    int pid = fork();
    if (pid == 0) {
        execl("./server", "./server", "-m", (char*) to_string(m).c_str(), nullptr);
    }

	// initialize overhead (including the control channel)
	FIFORequestChannel* chan = new FIFORequestChannel("control", FIFORequestChannel::CLIENT_SIDE);
    BoundedBuffer request_buffer(b);
    BoundedBuffer response_buffer(b);
	HistogramCollection hc;

    vector<thread> producers;
    vector<FIFORequestChannel*> FIFOs;
    vector<thread> workers;
    vector<thread> histograms;

    // making histograms and adding to collection
    for (int i = 0; i < p; i++) {
        Histogram* h = new Histogram(10, -2.0, 2.0);
        hc.add(h);
    }
	
	// record start time
    struct timeval start, end;
    gettimeofday(&start, 0);
/**/    
    // create all threads here 
    if (f.empty()) { // if DATA:
        for (int i = 1; i <= p; ++i) {
            producers.push_back(thread(patient_thread_function, i, n, m, &request_buffer));
        }
    } else if (!f.empty()) { // if FILE:
        producers.push_back(thread(file_thread_function, f, m, &request_buffer, chan));
    }

    for (int i = 0; i < w; ++i) {
        MESSAGE_TYPE np = NEWCHANNEL_MSG;
		chan->cwrite(&np, sizeof(MESSAGE_TYPE));
		char pipename[256];
		chan->cread(&pipename, 256);
		FIFORequestChannel* pipe = new FIFORequestChannel(pipename, FIFORequestChannel::CLIENT_SIDE);
        FIFOs.push_back(pipe); // push back the new channel onto the vector
        workers.push_back(thread(worker_thread_function, m, &request_buffer, &response_buffer, FIFOs.at(i), f));   
    }
    if (f.empty()) { // if DATA:
        for (int i = 0; i < h; ++i) {
            histograms.push_back(thread(histogram_thread_function, m, &response_buffer, &hc));
        }
    }

	// join all threads here 
    for (size_t i = 0; i < producers.size(); ++i) {
        producers.at(i).join();
    }
    
    if (DEBUG) cout << "Producers finished" << endl;
    char* msg = new char[m];
    datamsg qm(-1, 0, 0);
    qm.mtype = UNKNOWN_MSG;
    memcpy(msg, &qm, sizeof(datamsg));
    for (int i = 0; i < w; i++) { // Push Kill Msg to all Worker Threads
        if (DEBUG) cout << "Pushing Final Msg to Request" << endl;
        request_buffer.push(msg, sizeof(datamsg));
        if (DEBUG) cout << "Final Msg Pushed to Request" << endl;
    }
    delete [] msg;

    for (size_t i = 0; i < workers.size(); ++i) {
        workers.at(i).join();
    }
    
    if (DEBUG) cout << "Workers finished" << endl;
    bundle B;
    B.first = -1;
    B.second = -1;
    msg = new char[sizeof(B)];
    memcpy(msg, &B, sizeof(B));
    for (int i = 0; i < h; i++) { // Push Kill Msg to all Histogram Threads
        if (DEBUG) cout << "Pushing Final Msg to Response" << endl;
        response_buffer.push(msg, sizeof(B));
        if (DEBUG) cout << "Final Msg Pushed to Response" << endl;
    }
    delete [] msg;
    
    for (size_t i = 0; i < histograms.size(); ++i) {
        histograms.at(i).join();
    }

    if (DEBUG) cout << "Histograms finished" << endl;
//*/
	// record end time
    gettimeofday(&end, 0);

    // print the results
	if (f == "") {
		hc.print();
	}
    int secs = ((1e6*end.tv_sec - 1e6*start.tv_sec) + (end.tv_usec - start.tv_usec)) / ((int) 1e6);
    int usecs = (int) ((1e6*end.tv_sec - 1e6*start.tv_sec) + (end.tv_usec - start.tv_usec)) % ((int) 1e6);
    cout << "Took " << secs << " seconds and " << usecs << " micro seconds" << endl;

    MESSAGE_TYPE q = QUIT_MSG;
	for (size_t i = 0; i < FIFOs.size(); ++i) {
        FIFOs.at(i)->cwrite(&q, sizeof(MESSAGE_TYPE));
        delete FIFOs.at(i);
    }
    // quit and close control channel
    chan->cwrite ((char *) &q, sizeof (MESSAGE_TYPE));
    cout << "All Done!" << endl;
    delete chan;

	// wait for server to exit
	wait(nullptr);
}
