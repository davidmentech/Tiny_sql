#ifndef DB_H
#define DB_H
#define NUM_OF_OPS_FOR_GC 1000 //number of insert/delete operations after which GC is triggered
#define NUM_OF_OPS_FOR_GLOB_GC 5000
#include <unordered_map>
#include "BPlusTree.h"
bool check_Type(const string& value,const string& type){
    int size=value.size();
    if(size>=2 && value[0]=='\"'&&value[size-1]=='\"') return type=="S";
    stringstream ss(value);
    int num;
    if (ss >> num && ss.eof()) {
        return type=="I"; //if type is I and value is int return I else S
    } else {
        return false;
    }
}

class Schema{
public:
    string schema_name;
    unordered_map<string,int> column_names; //maybe change to map of keys to index
    vector<string> column_types; // "int" or "string" can be optimized for boolean
    int primary_key_size; //PK will always be at the start of the record and the size will signal how many columns are in the PK
    int number_of_columns;
    BPlusTree<vector<string>,streampos>* index_tree; //BPlus tree to manage the index // Count of insert/delete operations
    Schema(){}
    Schema(const vector<string>& command,const int& command_size,const string& schema_name):schema_name(schema_name),primary_key_size(0),number_of_columns(0){
        auto it=find(command.begin(),command.end(),"KEY");
        if(it==command.end()) throw invalid_argument("Primary key definition missing");
        ++it;
        if(it==command.end()) throw invalid_argument("Invalid primary key definition.(No columns specified)");
        int idx=2;
        while(idx<command_size && command[idx]!="KEY"){
            string col_def=command[idx];
            size_t pos=col_def.find(':');
            if(pos==string::npos){
                throw invalid_argument("Invalid column definition");
            }
            string col_name=col_def.substr(0,pos);
            string col_type=col_def.substr(pos+1);
            if(col_type!="I" && col_type!="S"){
                throw invalid_argument("Unsupported column type: "+col_type);
            }
            if(it!=command.end()){
                if(col_name!=*it) throw invalid_argument("Primary key columns must be at the start of the schema definition or columns dont match.");
                ++it;
                primary_key_size++;
            }
            if(column_names.find(col_name)!=column_names.end()) throw("column name "+col_name+" alredy exist");
            column_names[col_name]=idx;
            column_types.push_back(col_type);
            idx++;
            number_of_columns++;
        }
        if(primary_key_size>number_of_columns){
            throw invalid_argument("Primary key size exceeds number of columns.");
        }
        index_tree=new BPlusTree<vector<string>,streampos>(MIN_DEGREE,schema_name);
    }
    void add_record(const vector<string>& add_command,const int& command_size){
        if(command_size!=number_of_columns+3){ //INSERT val1 ... valn To table_name 
            throw invalid_argument("Invalid INSERT command (should be INSERT val1 ... valn TO table_name). where n is number of columns in table");
        }
        vector<string> key;
        for(int i=0;i<primary_key_size;i++){
            if(!check_Type(add_command[i+1],column_types[i])){ //+1 to skip "INSERT" 
                throw invalid_argument("Type mismatch in column number " +to_string(i+1));
            }
            if(column_types[i]=="S"){
               key.push_back(add_command[i+1].substr(1,add_command[i+1].size()-2));
            }
            //maybe remove the quatos from inside just for clairty
            else key.push_back(add_command[i+1]); //simple delimiter
        }
        //check if key already exists
        if(index_tree->search(key).has_value()){
            throw invalid_argument("Duplicate primary key.");
        }
        //serialize record to a single string
        vector<string> serialized_record;
        int data_offset=number_of_columns-2*primary_key_size; //
        for(int i=primary_key_size;i<number_of_columns;i++){ //maybe only write the data only without key
            if(!check_Type(add_command[i+1],column_types[i+data_offset])){ //+1 to skip "INSERT" 
                throw invalid_argument("Type mismatch in column number "+to_string(i+data_offset+1));
            }
            if(column_types[i+data_offset]=="S"){
              serialized_record.push_back(add_command[i+1].substr(1,add_command[i+1].size()-2));
            }
            else serialized_record.push_back(add_command[i+1]);
        }
        //write to file and get offset
        streampos offset=write_line_to_file(schema_name+"_data", serialized_record);
        //insert into bplus tree
        index_tree->insert(key, offset);
    }
    void remove_record(const vector<string>& delete_command,const int& command_size){
        if(command_size!=primary_key_size+3){ //DELETE val1 ... valn From table_name 
            throw invalid_argument("invalid DELETE command (should be DELETE val1 ... valn FROM table_name). where n is number of columns in primary key");
        }
        vector<string> key;
        for(int i=0;i<primary_key_size;i++){
            if(!check_Type(delete_command[i+1],column_types[i])){ //+1 to skip "DELETE" 
                throw invalid_argument("Type mismatch in column number "+to_string(i+1));
            }
            if(column_types[i]=="S"){
              key.push_back(delete_command[i+1].substr(1,delete_command[i+1].size()-2));
            }
            else key.push_back(delete_command[i+1]);
     //simple delimiter
        }
        //check if key exists
        optional<streampos> offset=index_tree->search(key);
        if(!offset.has_value()){
            throw invalid_argument("Record with given primary key does not exist.");
        }
        //remove from bplus tree
        index_tree->remove(key);

    }
    vector<vector<string>> get_all_data(vector<pair<vector<string>,streampos>> idx_tree_values){
        vector<vector<string>> result; 
            for(const auto& [key,offset]:idx_tree_values){
                vector<string> record=read_line_from_file(schema_name+"_data", offset);
                if(!record.empty()){
                    record.insert(record.begin(),key.begin(),key.end());
                    result.push_back(record);
                }
        }   
        return result;
    } 
    //maybe work all the cluases at once
    vector<vector<string>> apply_caluses(const vector<string>& select_command){ //the clauses are at the back
        //columns name must be on left and cluase must be without any spaces
        vector<vector<string>> filtered_values;
        vector<string> commands;
        bool look_up_index=false;
        vector<pair<vector<string>,streampos>> vals_from_index;
        string op="";
        string column;
        string val;
        int ind=select_command.size()-1;
        while (select_command[ind]!="WHERE"){
            string clause=select_command[ind];
            for(int i=0;i<clause.size()-1;i++){
                op=clause[i];
                op+=clause[i+1];
                if(op==">="||op=="<="||op=="=="||op=="!="){
                    column=clause.substr(0,i);
                    val=clause.substr(i+2,clause.size()-(i));
                    break; //val must be enterd with commas between the values if key bigger then one column
                }
            }
            
            if(column=="KEY"){
                if(look_up_index &&vals_from_index.empty()){ //this means we already looked up the index and we filtered all the values out
                    return filtered_values;
                }
                vector<string> key;
                string token;
                stringstream ss(val);
                int ind=0;
                while(getline(ss,token,',')){
                    if(ind>=primary_key_size) throw invalid_argument("invalid key value");
                    if(!check_Type(token,column_types[ind])) throw invalid_argument("Type mismatch in column number "+ind);
                    key.push_back(token);
                }
                if(!look_up_index){
                    if(op==">="){
                        vals_from_index=index_tree->rangeQuery(key,index_tree->get_Max());
                    }
                    else if(op=="<="){
                        vals_from_index=index_tree->rangeQuery(index_tree->get_Min(),key);
                    }
                    else if(op=="=="){
                        optional<streampos> val_from_indx=index_tree->search(key);
                        if(!val_from_indx.has_value()) return filtered_values;
                        else{
                            return get_all_data({std::make_pair(key, val_from_indx.value())});
                        }
                    }
                    else if(op=="!="){
                       vector<pair<vector<string>,streampos>> dummy =index_tree->getAllValues();
                       for(int i=0;i<dummy.size();i++){
                        if(dummy[i].first!=key){
                            vals_from_index.push_back(dummy[i]);
                        }
                       }
                    }
                    else{
                        throw invalid_argument("UNKONWN COMPARSION OPERATOR (SHOULD ONLY BE >= <= != ==)");
                    }
                    if (vals_from_index.empty()) return filtered_values;
                    look_up_index=true;
                }
                else{ // that's what left !vals_from_index.empty() && we looked up the index
                    if(op==">="){
                        auto it=lower_bound(vals_from_index.begin(),vals_from_index.end(),key,[](const auto& elem,const auto& val) {
                         return elem.first < val;
                        });
                        if(it==vals_from_index.end()) return filtered_values;
                        else{
                            vector<pair<vector<string>,streampos>> dummy;
                            while(it!=vals_from_index.end()){
                                dummy.push_back(*it);
                                ++it;
                            }
                            vals_from_index=dummy;
                        }
                    }
                    else if(op=="<="){
                       auto it = std::upper_bound(vals_from_index.begin(), vals_from_index.end(),key, 
                       [](const auto& val, const auto& elem) {
                        return val < elem.first;
                        });
                        if(it == vals_from_index.begin()) return filtered_values;
                        it=std::prev(it);
                        vector<pair<vector<string>,streampos>> dummy;
                        while(it!=vals_from_index.begin()){
                            dummy.push_back(*it);
                            --it;
                        }
                        dummy.push_back(*it);
                        vals_from_index=dummy;
                    }
                    else if(op=="=="){
                        auto it=lower_bound(vals_from_index.begin(),vals_from_index.end(),key,[](const auto& elem,const auto& val) {
                         return elem.first < val;
                        });
                        if(it==vals_from_index.end()) return filtered_values;
                        else{
                            if((*it).first!=key) return filtered_values;
                            return get_all_data({std::make_pair(key, (*it).second)});
                        }
                    }
                    else if(op=="!="){
                       vector<pair<vector<string>,streampos>> dummy;
                       for(int i=0;i<vals_from_index.size();i++){
                        if(vals_from_index[i].first!=key){
                            dummy.push_back(vals_from_index[i]);
                        }
                       }
                       vals_from_index=dummy;
                    }
                    else{
                        throw invalid_argument("UNKONWN COMPARSION OPERATOR (SHOULD ONLY BE >= <= != ==)");
                    }
                    if (vals_from_index.empty()) return filtered_values;
                }
            }
            else{
                commands.push_back(clause);
            }
            --ind;
        }
        try {
            vector<vector<string>> dummy;
            if (vals_from_index.empty()){
                dummy=get_all_data(index_tree->getAllValues());
            }
            else {
                dummy=get_all_data(vals_from_index);
            }
            for(const auto& clause:commands){
                for(int i=0;i<clause.size();i++){
                    op=clause[i];
                    op+=clause[i+1];
                    if(op==">="||op=="<="||op=="=="||op=="!="){
                        column=clause.substr(0,i);
                        val=clause.substr(i+2,clause.size()-(i));
                        break; //val must be enterd with commas between the values if key bigger then one column
                    }  
                }
                int idx=column_names.at(column);
                if(!check_Type(val,column_types[idx])) throw invalid_argument("value given doesnt match column "+column+" type");
                    if(op==">="){
                            for(const vector<string>& v : dummy){
                            if(v[idx]>=val) filtered_values.push_back(v);
                            }
                        }
                        else if(op=="<="){
                            for(const vector<string>& v : dummy){
                                if(v[idx]<=val) filtered_values.push_back(v);
                            }
                        }
                        else if(op=="=="){
                            for(const vector<string>& v : dummy){
                                if(v[idx]==val) filtered_values.push_back(v);
                            }
                        }
                        else if(op=="!="){
                            for(const vector<string>& v : dummy){
                                if(v[idx]!=val) filtered_values.push_back(v);
                            }
                        }
                        else{
                            throw invalid_argument("UNKONWN COMPARSION OPERATOR (SHOULD ONLY BE >= <= != ==)");
                        }
            if(filtered_values.empty()) return filtered_values;
            dummy=filtered_values;
            filtered_values.clear();
            }
            return dummy;
        }
        catch(const out_of_range& e){
           throw invalid_argument("the column "+column+" doesnt exist");
        }
    }  
    vector<string> select_records(const vector<string>& select_command,const int& command_size){
        //need to add priority to KEY clauses
        auto it=find(select_command.begin(),select_command.end(),"WHERE");
        vector<vector<string>> all_values;
        if(it!=select_command.end()){
            ++it;
            if(it==select_command.end()) throw invalid_argument("there is WHERE word but no clauses");
            all_values=apply_caluses(select_command);
        }
        else {
         all_values=get_all_data(index_tree->getAllValues());
        }
        vector<string> result;
        if(select_command[1]=="*"){
            //select all columns
            for(const vector<string>& v:all_values){
                string str="";
                for(int i=0;i<v.size();++i){
                    if(column_types[i]=="S") str+='\"'+v[i]+'\"'+" ";
                    else str+=v[i]+" ";

                }
                str.pop_back();
                result.push_back(str);
            }
            return result;
        } 
        else {
            vector<int> col_indices;
            for(auto it2 = select_command.begin()+1;it2!=it;it2++){
                try{
                    int idx=column_names.at(*it2);
                    col_indices.push_back(idx);
                }
                catch(const out_of_range& e){
                    throw invalid_argument("the column "+*it2+" doesnt exist");
                }
            }
            //retrieve all records and filter columns
            for(const vector<string>& v:all_values){
                string record="";
                for(int idx:col_indices){
                    if(column_types[idx]=="S") record+='\"'+v[idx]+'\"'+" ";
                    record+=v[idx]+" ";
                }
                record.pop_back();
                result.push_back(record);
            }
        }
        return result;
    }
    void GC(){
        if(!filesystem::exists("DB_files/"+schema_name+"_data.txt")) return;
        vector<pair<vector<string>,streampos>> all_values=index_tree->getAllValues();
        vector<streampos> offsets;
        for (const auto& [key,offset]:all_values){
            vector<string> record=read_line_from_file(schema_name+"_data", offset);
            streampos new_offset=write_line_to_file(schema_name+"_data_temp", record);
            offsets.push_back(new_offset);
        }
        index_tree->GC_with_values(offsets);
        //replace old data file with new compacted file
        if(remove(("DB_files/"+schema_name+"_data.txt").c_str())!=0){
            cerr<<"Error deleting old data file during GC."<<endl;
        }
        if(rename(("DB_files/"+schema_name+"_data_temp.txt").c_str(),("DB_files/"+schema_name+"_data.txt").c_str())!=0){
            cerr<<"Error renaming temp data file during GC."<<endl;
        }
        //serialize the tree to update offsets
        index_tree->serialize_Tree();
    }
void desrialize_Schema(){
    index_tree->deserialize_Tree();
}
};




class DB{
public:
    unordered_map<string, Schema> schemas;
    int number_of_ops; //map of table name to row count
    DB():number_of_ops(0){}
    void create_table(const vector<string>& create_command){
        int command_size=create_command.size();
        if(command_size<5){ //needed CREATE table_name col1:type1 ... Key col1 ... at least 5 tokens
            throw invalid_argument("Invalid create command");
        }
        if(schemas.find(create_command[1])!=schemas.end()){
            throw invalid_argument("Table "+create_command[1]+" already exists.");
        }
        Schema schema(create_command,command_size,create_command[1]);
        schemas[create_command[1]]=schema;
        ofstream serilaize_file("DB_files/DB.txt",ios::app);
        for(int i=0;i<command_size-1;i++){
            serilaize_file<<create_command[i]<<" ";
        }
        serilaize_file<<create_command[command_size-1]<<endl;
        serilaize_file.close();
    }
    void write_to_journal(const vector<string>& command){ //used for inserts and deletions only
        ofstream journal_file("DB_files/DB_journal.txt",ios::app);
        for(int i=0;i<command.size()-1;i++){
            journal_file<<command[i]<<" ";
        }
        journal_file<<command[command.size()-1]<<endl;
        journal_file.close();
    }
    void add_record(const vector<string>& add_command){ //INSERT val1 val2 ... To table_name
        int command_size=add_command.size();
        if(find(add_command.begin(),add_command.end(),"TO")==add_command.end()){ //needed INSERT val1 ... To table_name at least 4 tokens
            throw invalid_argument("missing TO in insert command");
        }
        string table_name=add_command[command_size-1];
        if(table_name=="TO"){
            throw invalid_argument("Table name missing in insert command.");
        } //last token is table name
        if(schemas.find(table_name)==schemas.end()){
            throw invalid_argument("Table "+table_name+" does not exist.");
        }
        schemas[table_name].add_record(add_command,command_size);
        number_of_ops++;
        if(number_of_ops>NUM_OF_OPS_FOR_GLOB_GC) GC();
        else{         
                write_to_journal(add_command);
        }
    }
    void remove_record(const vector<string>& delete_command){
        int command_size=delete_command.size();
        if(find(delete_command.begin(),delete_command.end(),"FROM")==delete_command.end()){ //needed DELETE val1 ... From table_name at least 4 tokens
            throw invalid_argument("missing FROM in delete command");
        }
        string table_name=delete_command[command_size-1];
        if(table_name=="FROM"){
            throw invalid_argument("Table name missing in delete command.");
        } //last token is table name
        if(schemas.find(table_name)==schemas.end()){
            throw invalid_argument("Table "+table_name+" does not exist.");
        }
        schemas[table_name].remove_record(delete_command,command_size);
        number_of_ops++;
        if(number_of_ops>=NUM_OF_OPS_FOR_GLOB_GC) GC(); //intiate global GC
        else{
                write_to_journal(delete_command);
        }
    }
    vector<string> select_records(const vector<string>& select_command){
         int command_size=select_command.size();
         auto it=find(select_command.begin(),select_command.end(),"FROM");
        if(command_size<4||find(select_command.begin(),select_command.end(),"FROM")==select_command.end()){ //needed SELECT column FROM table_name at least 4 tokens
            throw invalid_argument("Invalid select command");
        }
        ++it;
        if(it==select_command.end()) throw invalid_argument("Table name missing in select command.");
        string table_name=*it;
        if(table_name=="WHERE"){
            throw invalid_argument("Table name missing in select command.");
        } //last token is table name
        if(schemas.find(table_name)==schemas.end()){
            throw invalid_argument("Table "+table_name+" does not exist.");
        }
        return schemas[table_name].select_records(select_command,command_size);
    }
    void GC(){
        for(auto& [table_name,schema]:schemas){
                schema.GC(); 
        }
        number_of_ops=0;
        if(filesystem::exists("DB_files/DB_journal.txt")){
            filesystem::remove("DB_files/DB_journal.txt");
        }
    }
    void deserialize_DB(){
        ifstream file("DB_files/DB.txt");
        string command;
        while(getline(file,command)){
            stringstream ss(command);
            string token;
            vector<string> create_command;
            while(getline(ss,token,' ')){
                create_command.push_back(token);
            }
            Schema schema(create_command,create_command.size(),create_command[1]);
            schemas[create_command[1]]=schema;
            schema.desrialize_Schema();
        }
        file.close();
        if(filesystem::exists("DB_files/DB_journal.txt")){
            ifstream journal("DB_files/DB_journal.txt");
            string command;
            while(getline(journal,command)){
                stringstream ss(command);
                string token;
                vector<string> command_vec;
                while(getline(ss,token,' ')){
                    command_vec.push_back(token);
                }
                if(command_vec[0]=="INSERT"){ //can only be insert and delete
                    add_record(command_vec);
                }
                else{
                    remove_record(command_vec);
                }
            }
            journal.close();
            filesystem::remove("DB_files/DB_journal.txt");
        }
    }
    void clear(){
        schemas.clear();
        number_of_ops=0;
    }

};





#endif