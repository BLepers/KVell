#include <iostream>

#include <fstream>
#include <sstream>
#include <string>
using namespace std;
struct e_t{
   uint32_t src;
   uint32_t dst;
};



int main(int argc, char** argv) {

   std::string fname = argv[1];
   int iops = atoi(argv[2]);

   ifstream source;                    // build a read-Stream
   if (!source)  {                     // if it does not work
      cerr << "Can't open Data!\n";
   }

   int counter = 0;
   double total_time = 0;
   double last_time = 0.0;
   double times_read = 0.01;
   uint64_t total_data = 0;
   uint64_t time_written = 0;
   source.open(fname, ios_base::in);
   for(std::string line; std::getline(source, line); )   //read stream line by line
   {

      std::string line2;
      std::istringstream in(line);

      double time, time2;
      uint32_t data,data2;
      in >> time >> data ;       //now read the whitespace-separated floats
      if(iops)
         total_data ++;
      else
         total_data += data;
      total_time += time - last_time;
      if(total_time >= times_read) {
         while(times_read < total_time - 0.01) {
            std::cout << times_read << " " << 0 << "\n";
            times_read += 0.01;
         }
         if(iops)
            std::cout << total_time << " " << (total_data /2.* 100.) << "\n" ; ///2 * 100)/1024 << "\n";
         else
            std::cout << total_time << " " << (total_data /2.* 100.) /1024. << "\n" ; ///2 * 100)/1024 << "\n";
         times_read =  time + 0.01;
         total_data = 0;
      }
      last_time = time;
   }
   return 0;
}
