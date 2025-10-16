#include "BPlusTree.h"
#include "DB.h"
DB db;


void parse_command(const string& command) {
    vector<string> tokens;
    stringstream ss(command);
    string token;
    while(getline(ss,token,' ')){
        tokens.push_back(token);
    }
    if (tokens.empty()) return;
    string cmd = tokens[0];
    if(cmd=="CREATE"){   
            db.create_table(tokens);
            cout<<"Table created successfully."<<endl;
    }
    else if(cmd=="INSERT")
    {
            db.add_record(tokens);
            cout<<"Record inserted successfully."<<endl;
    }
    else if(cmd=="DELETE")
    {
            db.remove_record(tokens);
            cout<<"Record deleted successfully."<<endl;
    }
    else if(cmd=="SELECT")
    {
        
            vector<string> results=db.select_records(tokens);
            if(results.empty()){
                cout<<"No records found."<<endl;
            }
            else{
                for(const string& rec:results){
                    cout<<rec<<endl;
                }
            }
    }
    else if (cmd=="GC"){
        //call garbage collector
        db.GC();
        cout<<"Garbage collection completed."<<endl;
    }
    else if (cmd=="EXIT")
    {
        db.GC(); //final GC before exit
        cout<<"Exiting program."<<endl;
        exit(0);
        //add gc and serialization here
    }
    else{
        throw invalid_argument("Unknown command: "+cmd);
    }
}
int main(){
    cout<<"Hello and welcome to SQL_lite"<<endl;
    cout<<"Would you like to load the last DB created?(Y/N)"<<endl;
    string line;
    while(getline(cin,line)){
          if(line=="Y"){
               if(filesystem::exists("DB_files")){
                    db.deserialize_DB();
                    cout<<"Data restored"<<endl;
               }
               else cout<<"there is no state saved so starting over"<<endl;
               break;
          }
          else if(line=="N"){
               if(filesystem::exists("DB_files")){
                    filesystem::remove_all("DB_files");
               }     
               filesystem::create_directory("DB_files");
               break;
          }
          else{
               cout<<"YOU MUST ENTER Y OR N"<<endl;
          }
    }
    //reload data from files
    while(true){
        cout<<"Enter Command (create,insert,delete,select)"<<endl;
        getline(cin,line);
        parse_command(line);
    }
}