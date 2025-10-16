#include "../src/BPlusTree.h"
#include "../src/DB.h"
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
            //cout<<"Table created successfully."<<endl;
    }
    else if(cmd=="INSERT")
    {
            db.add_record(tokens);
            //cout<<"Record inserted successfully."<<endl;
    }
    else if(cmd=="DELETE")
    {
            db.remove_record(tokens);
            //cout<<"Record deleted successfully."<<endl;
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
        //cout<<"Garbage collection completed."<<endl;
    }
    else if (cmd=="EXIT")
    {
        db.GC(); //final GC before exit
        //cout<<"Exiting program."<<endl;
        //add gc and serialization here
    }
    else{
        throw invalid_argument("Unknown command: "+cmd);
    }
}
#include <stdexcept>

// Using a macro for the expected failure test to reduce code duplication
#define RUN_FAILURE_TEST(line_str, expected_error_str) \
   line = line_str; \
   try { \
       parse_command(line); \
       /* If parse_command doesn't throw, it's a failure */ \
       throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error_str); \
   } \
   catch (const std::invalid_argument& e) { \
       if (std::string(e.what()) == expected_error_str) { \
           std::cout << "Success in TEST " << line << std::endl; \
       } else { \
           throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + e.what() + " Wanted: " + expected_error_str); \
       } \
   }

int main() {
    filesystem::create_directory("DB_files");
    std::string line;
    std::string expected_error;

    // Test 1: Successful CREATE
    line = "CREATE A A:I B:S KEY A";
    try {
        parse_command(line);
        std::cout << "Success in TEST " << line << std::endl;
    }
    catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + e.what() + " Wanted: Success");
    }

    // Test 2: CREATE table that already exists
    line = "CREATE A A:S B:S KEY A";
    expected_error = "Table A already exists.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 3: CREATE with unsupported column type
    line = "CREATE B A:P B:S KEY A";
    expected_error = "Unsupported column type: P";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 4: CREATE with primary key not at the start
    line = "CREATE B A:I B:S KEY B";
    expected_error = "Primary key columns must be at the start of the schema definition or columns dont match.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 5: CREATE with a non-existent column as primary key
    line = "CREATE B A:I B:S KEY C";
    expected_error = "Primary key columns must be at the start of the schema definition or columns dont match.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 6: CREATE with no columns specified for the key
    line = "CREATE B A:I B:S KEY";
    expected_error = "Invalid primary key definition.(No columns specified)";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 7: CREATE with missing KEY keyword
    line = "CREATE B A:I B:S C:S";
    expected_error = "Primary key definition missing";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 8: Successful CREATE of table B
    line = "CREATE B A:I B:S KEY A";
    try {
        parse_command(line);
        std::cout << "Success in TEST " << line << std::endl;
    }
    catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + e.what() + " Wanted: Success");
    }

    // Test 9: INSERT into a non-existent table
    line = "INSERT 54 \"hello\" TO C";
    expected_error = "Table C does not exist.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 10: INSERT with type mismatch
    line = "INSERT \"hello\" \"hello\" TO A";
    expected_error = "Type mismatch in column number 1";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 11: Successful INSERT
    line = "INSERT 54 \"hello\" TO A";
    try {
        parse_command(line);
        std::cout << "Success in TEST " << line << std::endl;
    }
    catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + e.what() + " Wanted: Success");
    }

    // Test 12: INSERT with duplicate primary key
    line = "INSERT 54 \"hello\" TO A";
    expected_error = "Duplicate primary key.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 13: INSERT missing 'TO' keyword
    line = "INSERT 54 \"hello\" A";
    expected_error = "missing TO in insert command";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 14: INSERT missing table name
    line = "INSERT 54 \"hello\" TO";
    expected_error = "Table name missing in insert command.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 15: Successful DELETE
    line = "DELETE 54 FROM A";
    try {
        parse_command(line);
        std::cout << "Success in TEST " << line << std::endl;
    }
    catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + e.what() + " Wanted: Success");
    }

    // Test 16: DELETE from non-existent table
    line = "DELETE 54 FROM C";
    expected_error = "Table C does not exist.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 17: DELETE a non-existent record
    line = "DELETE 54 FROM B";
    expected_error = "Record with given primary key does not exist.";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // Test 18: DELETE missing 'FROM' keyword
    line = "DELETE 54 A";
    expected_error = "missing FROM in delete command";
    try {
        parse_command(line);
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: Success Wanted: " + expected_error);
    }
    catch (const std::invalid_argument& e) {
        if (std::string(e.what()) == expected_error) {
            std::cout << "Success in TEST " << line << std::endl;
        } else {
            throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + std::string(e.what()) + " Wanted: " + expected_error);
        }
    }

    // --- Subsequent Operations (Assuming they should succeed) ---

    try {
        parse_command("INSERT 54 \"hello\" TO A");
        parse_command("INSERT 55 \"world\" TO A");
        std::cout << "Success in subsequent INSERT operations" << std::endl;
    } catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL during subsequent INSERTs: " + std::string(e.what()));
    }

    line = "SELECT * FROM A";
    try {
        parse_command(line);
        std::cout << "Success in TEST " << line << std::endl;
    } catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + e.what() + " Wanted: Success");
    }

    try {
        parse_command("EXIT");
        db.clear();
        db.deserialize_DB();
        std::cout << "Success in EXIT, clear, and deserialize" << std::endl;
    } catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL during EXIT/deserialize: " + std::string(e.what()));
    }

    line = "SELECT * FROM A WHERE KEY==54";
    try {
        parse_command(line);
        std::cout << "Success in TEST " << line << std::endl;
    } catch (const std::invalid_argument& e) {
        throw std::invalid_argument("FAIL IN TEST: " + line + " Got: " + e.what() + " Wanted: Success");
    }
    filesystem::remove_all("DB_files");
    return 0;
}