#ifndef BPLUS_TREE_H
#define BPLUS_TREE_H
#define MIN_DEGREE 3 //MINIMUM DEGREE OF BPLUS TREE
#include <algorithm>
#include <queue>
#include <iostream>
#include <vector>
#include <fstream>
#include <string>
#include <optional>
#include <sstream>
#include <filesystem>
#include <ostream>
#include <type_traits>
using namespace std;
//in order to use the B_tree using special types you must add them to this conversion functions
template<typename>
inline constexpr bool always_false = false;
template<typename T>
struct is_vector : std::false_type {};
template<typename T, typename A>
struct is_vector<std::vector<T, A>> : std::true_type {};

// ==================== Refined String -> Type ====================

template<typename Type>
Type String_to_Type(const std::string& s) {
    if constexpr (is_vector<Type>::value) {
        using Elem = typename Type::value_type; 
        Type result;
        std::stringstream ss(s);
        std::string token;
        while (std::getline(ss, token, ' ')) {
            if (!token.empty()) {
                result.push_back(String_to_Type<Elem>(token));
            }
        }
        return result;
    }
    else {
        if constexpr (std::is_same_v<Type, std::string>) {
            return s;
        }
        else if constexpr (std::is_integral_v<Type>) {
            return static_cast<Type>(std::stoll(s));
        }
        else if constexpr (std::is_same_v<Type, std::streampos>) {
            return std::streampos(std::stoll(s));
        }
        else if constexpr (std::is_floating_point_v<Type>) {
            return static_cast<Type>(std::stod(s));
        }
        else {
            static_assert(always_false<Type>, "Unsupported type in String_to_Type");
        }
    }
}

// ==================== Refined Type -> String ====================

template<typename Type>
std::string Type_to_String(const Type& value) {
    if constexpr (is_vector<Type>::value) {
        std::string result;
        for (size_t i = 0; i < value.size(); ++i) {
            if (i > 0) result += " ";
            result += Type_to_String(value[i]);
        }
        return result;
    }
    else {
        if constexpr (std::is_same_v<Type, std::string>) {
            return value;
        }
        else if constexpr (std::is_integral_v<Type> || std::is_floating_point_v<Type>) {
            return std::to_string(value);
        }
        else if constexpr (std::is_same_v<Type, std::streampos>) {
            return std::to_string(static_cast<long long>(value));
        }
        else {
            static_assert(always_false<Type>, "Unsupported type in Type_to_String");
        }
    }
}


// B plus tree class
//T is index type and S value type
template <typename T,typename S> class BPlusTree {
public: //maybe change to private later
    // structure to create a node
    struct Node {
        bool isLeaf;
        streampos offset;     // Offset in file (if needed) only for leaf nodes
        vector<T> keys;
        vector<Node*> children;
        Node* next; 
        Node(bool leaf = false) : isLeaf(leaf),offset(streampos(-1)) ,next(nullptr) {}
    };

    Node* root;
    int t; // Minimum degree
    string file_name;
    // Helper functions for insertion
    void splitChild(Node* parent, int index, Node* child);
    void insertNonFull(Node* node, const T& key, const S& value);
    // Helper functions for removal
    void removeInternal(Node* node, const T& key);
    void borrowFromPrev(Node* node, int index);
    void borrowFromNext(Node* node, int index);
    void merge(Node* node, int index);
    int findKey(Node* node, const T& key);
    void fill(Node* node, int index);
    // Helper for printing
    void printTree(Node* node, int level);
public:
    BPlusTree(int degree=MIN_DEGREE,const string& file_name=""): root(nullptr), t(degree), file_name(file_name+"_BPlusTree") {
    }
    void insert(const T& key, const S& value);
    optional<S> search(const T& key);
    void remove(const T& key);
    T findSmallestInSubtree(Node *node);
    vector<T> rangeQueryKeys(const T &lower, const T &upper);
    vector<pair<T, S>> rangeQuery(const T &lower, const T &upper);
    vector<T> getAllKeys();
    vector<pair<T, S>> getAllValues();
    void printTree();
    T get_Max();
    T get_Min();
    ~BPlusTree() {
        // Destructor to free memory
        // Implement a recursive function to delete all nodes
        function<void(Node*)> deleteNode = [&](Node* node) {
            if (node) {
                for (Node* child : node->children) {
                    deleteNode(child);
                }
                delete node;
            }
        };
        deleteNode(root);
    }
    void GC_with_values(vector<S> values);
    void serialize_Tree();
    void deserialize_Tree();
};

// Maybe dont need and can be saved in bTree
vector<string> read_line_from_file(const string& file,streampos offset) {
   vector<string> result;
    ifstream infile("DB_files/"+file+".txt", ios::binary);
    if (!infile || offset == streampos(-1)) {
        // cerr << "Error opening file or invalid offset: " << file << endl;
        return result;
    }
    infile.seekg(offset);
    string line;
    if (getline(infile, line)) {
        stringstream ss(line);
        string token;
        while (ss >> token) {
            result.emplace_back(token);
        }
    }
    infile.close();
    return result;
}
streampos write_line_to_file(const string& file,const vector<string>& data) {
    ofstream outfile("DB_files/"+file+".txt", ios::binary | ios::app);
    if (!outfile) {
        cerr << "Error opening file for writing: " << file << endl;
        return streampos(-1);
    }
    // Get the position before writing, this will be the offset for this new data block.
    std::ifstream in("DB_files/"+file+".txt", std::ios::binary | std::ios::ate);
    streampos position = in.tellg();
    int size_minus_one = data.size() - 1;
    if(size_minus_one>=0)
    {
        for (int i = 0; i < size_minus_one; i++) {
            outfile << data[i] << " ";
        }
        outfile << data[size_minus_one] << "\n";
    }
    outfile.close();
    return position;
}
//Insertion helper functions
template <typename T, typename S>
void BPlusTree<T, S>::splitChild(Node* parent, int index, Node* child) {
    Node* new_child = new Node(child->isLeaf);
    T middle_key = child->keys[t - 1];
    if (child->isLeaf) {

        // Move the second half of the keys to the new node
        new_child->keys.assign(child->keys.begin() + t -1, child->keys.end());
        child->keys.resize(t - 1);

        // Handle the data in the file
        vector<string> data = read_line_from_file(file_name, child->offset);
        vector<string> new_child_data(data.begin() + t - 1, data.end());
        data.resize(t - 1);

        // Write the split data back to the file in new locations
        child->offset = write_line_to_file(file_name, data);
        new_child->offset = write_line_to_file(file_name, new_child_data);

        // Link the leaves
        new_child->next = child->next;
        child->next = new_child;
    } else { // Internal node split
        // Move keys and children to the new node
        new_child->keys.assign(child->keys.begin() + t, child->keys.end());
        child->keys.resize(t-1); // Remove middle key and second half
        
        //assign greater than middle keys to new child
        new_child->children.assign(child->children.begin() + t, child->children.end());
        child->children.resize(t);
    }
    // Insert the middle key into the parent node and link the new child
    parent->keys.insert(parent->keys.begin() + index, middle_key);
    parent->children.insert(parent->children.begin() + index + 1, new_child);
}
// Implementation of insertNonFull function
template <typename T,typename S>
void BPlusTree<T,S>::insertNonFull(Node* node, const T& key, const S& value)
{
     if (node->isLeaf) {
        // Find position to insert the key into vector of keys
        auto it = upper_bound(node->keys.begin(), node->keys.end(), key);
        int insert_pos = distance(node->keys.begin(), it);
        node->keys.insert(it, key);
        //insert value to the file
        vector<string> data = read_line_from_file(file_name, node->offset);
        data.insert(data.begin() + insert_pos, Type_to_String(value) );
        node->offset = write_line_to_file(file_name, data);
    } else {
        //maybe can be optimized with binary search
        int i;
        auto it = upper_bound(node->keys.begin(),node->keys.end(),key);
        if(it==node->keys.end()){ i=node->keys.size();} //here is key bigger then everything
        else {i=distance(node->keys.begin(),it);}
        //the original code
        // int i = node->keys.size() - 1;
        // while (i >= 0 && key < node->keys[i]) {
        //     i--;
        // }
        //i++;
        // If the found child is full, then split it
        if (node->children[i]->keys.size() == 2 * t - 1) {
            splitChild(node, i, node->children[i]);
            if (key > node->keys[i]) {
                i++;
            }
        }
        insertNonFull(node->children[i], key, value);
    }
}
template <typename T,typename S> 
void BPlusTree<T,S>::insert(const T& key, const S& value)
{
    if (root == nullptr) {
        root = new Node(true); // Create a new leaf root
        root->keys.push_back(key);
        // Write the first data block to the file
        vector<string> data = {Type_to_String(value)};
        root->offset = write_line_to_file(file_name, data);
    } else {
        if (root->keys.size() == 2 * t - 1) {
            Node* newRoot = new Node(false); // New root is an internal node
            newRoot->children.push_back(root);
            splitChild(newRoot, 0, root);
            root = newRoot;
        }
        insertNonFull(root, key, value);
    }
}
// Search function implementation
template <typename T, typename S>
optional<S> BPlusTree<T, S>::search(const T& key) {
    Node* current = root;
    if (current == nullptr) return nullopt;

    while (!current->isLeaf) {
        // Find the first key that is >= our search key
        auto it = lower_bound(current->keys.begin(), current->keys.end(), key);
        int i = distance(current->keys.begin(), it);
        
        // If the key is found in an internal node, we must go to the right child
        if (i < current->keys.size() && current->keys[i] == key) {
             current = current->children[i+1];
        } else {
             current = current->children[i];
        }
    }

    // Now at a leaf node, search for the key
    auto it = lower_bound(current->keys.begin(), current->keys.end(), key);
    if (it != current->keys.end() && *it == key) {
        int key_pos = distance(current->keys.begin(), it);
        vector<string> data = read_line_from_file(file_name, current->offset);
        if (key_pos < data.size()) {
            return String_to_Type<S>(data[key_pos]);
        }
    }

    return nullopt;
}

// can be optimized to only check first and last key of each leaf node and if both in range, read whole leaf and line
template <typename T, typename S>
vector<T> BPlusTree<T, S>::rangeQueryKeys(const T& lower, const T& upper) {
    vector<T> result;
    if (root == nullptr) return result;

    Node* current = root;
    // Traverse to the first leaf node that might contain the lower bound
    while (!current->isLeaf) {
        auto it = upper_bound(current->keys.begin(), current->keys.end(), lower);
        int i = distance(current->keys.begin(), it);
        current = current->children[i];
    }
    // Scan through leaf nodes
    while (current != nullptr) {
        if(current->keys.empty() || current->keys.front() > upper) {
            break; // No more keys in range
        }
        if(current->keys.back() <= upper &&current->keys.front()>=lower) {
            for (int i = 0; i < current->keys.size(); i++) {
                result.push_back(current->keys[i]);
            }
        }
        else{
                auto it = upper_bound(current->keys.begin(), current->keys.end(), lower);
                auto dist = distance(current->keys.begin(), it);
                auto a=dist-1;
                if(dist>0&&current->keys[a]==lower){
                    result.push_back(current->keys[a]);
                }
                for (int i = dist;  i < current->keys.size(); i++) {
                    if(current->keys[i] > upper) break;
                    result.push_back(current->keys[i]);
                }
        }
        current = current->next;
    }
    return result;
}


template <typename T, typename S>
vector<pair<T, S>> BPlusTree<T, S>::rangeQuery(const T& lower, const T& upper) {
    vector<pair<T, S>> result;
    if (root == nullptr) return result;

    Node* current = root;
    // Traverse to the first leaf node that might contain the lower bound
    while (!current->isLeaf) {
        auto it = upper_bound(current->keys.begin(), current->keys.end(), lower);
        int i = distance(current->keys.begin(), it);
        current = current->children[i];
    }
    // Scan through leaf nodes
    while (current != nullptr) {
        if(current->keys.empty() || current->keys.front() > upper) {
            break; // No more keys in range
        }
        vector<string> data = read_line_from_file(file_name, current->offset);
        if(current->keys.back() <= upper &&current->keys.front()>=lower) {
            for (int i = 0; i < data.size(); i++) {
                result.push_back(make_pair(current->keys[i], String_to_Type<S>((data[i]))));
            }
        }
        else{
                auto it = upper_bound(current->keys.begin(), current->keys.end(), lower);
                auto dist = distance(current->keys.begin(), it);
                auto a=dist-1;
                if(dist>0&&current->keys[a]==lower){
                    result.push_back(make_pair(current->keys[a], String_to_Type<S>((data[a]))));
                }
                for (int i = dist;  i < data.size(); i++) {
                    if(current->keys[i] > upper) break;
                    result.push_back(make_pair(current->keys[i], String_to_Type<S>((data[i]))));
                }
        }
        current = current->next;
    }
    return result;
}
template <typename T, typename S>
vector<pair<T, S>> BPlusTree<T, S>::getAllValues(){
    vector<pair<T, S>> result;
    if (root == nullptr) return result;
    Node* current = root;
    while (!current->isLeaf) {
        current = current->children[0];
    }
    while (current != nullptr) {
        vector<string> data = read_line_from_file(file_name, current->offset);
        for (int i = 0; i < data.size(); i++)
        {
            result.push_back(make_pair(current->keys[i], String_to_Type<S>((data[i]))));
        }
        
        current = current->next;
    }
    return result;
}

template <typename T, typename S>
vector<T> BPlusTree<T, S>::getAllKeys(){
    vector<T> result;
    if (root == nullptr) return result;
    Node* current = root;
    while (!current->isLeaf) {
        current = current->children[0];
    }
    while (current != nullptr) {
       
        result.insert(result.end(),current->keys.begin(),current->keys.end()); 
        current = current->next;
    }
    return result;
}
// Removal function implementation
template <typename T,typename S>
void BPlusTree<T,S>::remove(const T& key)
{
     if (!root) {
        cout << "The tree is empty\n";
        return;
    }
    removeInternal(root, key);

    if (root->keys.empty() && !root->isLeaf) {
        Node* tmp = root;
        root = root->children[0];
        delete tmp;
    }
}
template <typename T, typename S>
T BPlusTree<T, S>::findSmallestInSubtree(Node* node) {
    Node* current = node;
    while (!current->isLeaf) {
        current = current->children.front();
    }
    return current->keys.front();
}

template <typename T, typename S>
void BPlusTree<T, S>::removeInternal(Node* node, const T& key) {
    int idx = findKey(node, key);
    if (node->isLeaf) {
        if (idx < node->keys.size() && node->keys[idx] == key) {
            node->keys.erase(node->keys.begin() + idx);
            vector<string> data = read_line_from_file(file_name, node->offset);
            data.erase(data.begin() + idx);
            node->offset = write_line_to_file(file_name, data);
        }
        return;
    }
    else{
        if (idx < node->keys.size() && node->keys[idx] == key) {
             removeInternal(node->children[idx + 1], key);
        } else {
             removeInternal(node->children[idx], key);
        }
    }// Key is not in this internal node, recurse down.
        Node* child = node->children[idx];
        if (child->keys.size() < t) {
            fill(node, idx);
        }
        if (idx > 0) {
            node->keys[idx - 1] = findSmallestInSubtree(node->children[idx]);
        }
}
template <typename T, typename S>
int BPlusTree<T, S>::findKey(Node* node, const T& key) {
    auto it = lower_bound(node->keys.begin(), node->keys.end(), key);
    return distance(node->keys.begin(), it);
}
template <typename T, typename S>
void BPlusTree<T, S>::fill(Node* node, int index) {
    if (index > 0 && node->children[index - 1]->keys.size() >= t) {
        borrowFromPrev(node, index);
    } else if (index < node->keys.size() && node->children[index + 1]->keys.size() >= t) {
        borrowFromNext(node, index);
    } else {
        if (index < node->keys.size()) {
            merge(node, index);
        } else {
            merge(node, index - 1);
        }
    }
}

template <typename T, typename S>
void BPlusTree<T, S>::borrowFromPrev(Node* node, int index) {
    Node* child = node->children[index];
    Node* sibling = node->children[index - 1];

    if (child->isLeaf) {
        // Take last key from sibling.
        child->keys.insert(child->keys.begin(), sibling->keys.back());
        sibling->keys.pop_back();

        // Update parent key to the new first key of child
        node->keys[index-1] = child->keys.front();

        // Update data files
        auto child_data = read_line_from_file(file_name, child->offset);
        auto sibling_data = read_line_from_file(file_name, sibling->offset);
        child_data.insert(child_data.begin(), sibling_data.back());
        sibling_data.pop_back();
        child->offset = write_line_to_file(file_name, child_data);
        sibling->offset = write_line_to_file(file_name, sibling_data);
    } else {
        // For internal nodes
        child->keys.insert(child->keys.begin(), node->keys[index - 1]);
        child->children.insert(child->children.begin(), sibling->children.back());
        node->keys[index - 1] = sibling->keys.back();
        sibling->keys.pop_back();
        sibling->children.pop_back();
    }
}

template <typename T, typename S>
void BPlusTree<T, S>::borrowFromNext(Node* node, int index) {
    Node* child = node->children[index];
    Node* sibling = node->children[index + 1];

    if (child->isLeaf) {
        child->keys.push_back(sibling->keys.front());
        sibling->keys.erase(sibling->keys.begin());

        // Update parent key to new first key of sibling
        node->keys[index] = sibling->keys.front();
        
        // Update data files
        auto child_data = read_line_from_file(file_name, child->offset);
        auto sibling_data = read_line_from_file(file_name, sibling->offset);
        child_data.push_back(sibling_data.front());
        sibling_data.erase(sibling_data.begin());
        child->offset = write_line_to_file(file_name, child_data);
        sibling->offset = write_line_to_file(file_name, sibling_data);

    } else {
        // For internal nodes
        child->keys.push_back(node->keys[index]);
        child->children.push_back(sibling->children.front());
        node->keys[index] = sibling->keys.front();
        sibling->keys.erase(sibling->keys.begin());
        sibling->children.erase(sibling->children.begin());
    }
}

template <typename T, typename S>
void BPlusTree<T, S>::merge(Node* node, int index) {
    Node* child = node->children[index];
    Node* sibling = node->children[index + 1];

    if (child->isLeaf) {
        // Append sibling's keys to child's keys
        child->keys.insert(child->keys.end(), sibling->keys.begin(), sibling->keys.end());
        
        // Merge data from files
        auto child_data = read_line_from_file(file_name, child->offset);
        auto sibling_data = read_line_from_file(file_name, sibling->offset);
        child_data.insert(child_data.end(), sibling_data.begin(), sibling_data.end());
        child->offset = write_line_to_file(file_name, child_data);

        // Update linked list
        child->next = sibling->next;
    } else {
        // For internal nodes, pull down key from parent
        child->keys.push_back(node->keys[index]);
        child->keys.insert(child->keys.end(), sibling->keys.begin(), sibling->keys.end());
        child->children.insert(child->children.end(), sibling->children.begin(), sibling->children.end());
    }

    // Remove key and pointer from parent
    node->keys.erase(node->keys.begin() + index);
    node->children.erase(node->children.begin() + index + 1);
    delete sibling;
}


// --- PRINTING ---

template <typename T, typename S>
void BPlusTree<T, S>::printTree(Node* node, int level) {
    if (node != nullptr) {
        cout << string(level * 4, ' ');
        cout << "[";
        for (size_t i = 0; i < node->keys.size(); ++i) {
            cout << node->keys[i] << (i == node->keys.size() - 1 ? "" : ",");
        }
        cout << "]";
        if (node->isLeaf) {
            cout << " (Leaf, Offset: " << node->offset << ")" << endl;
        } else {
            cout << " (Internal)" << endl;
            for (Node* child : node->children) {
                printTree(child, level + 1);
            }
        }
    }
}

template <typename T, typename S>
void BPlusTree<T, S>::printTree() {
    cout << "--- B+ Tree Structure ---" << endl;
    if (root != nullptr) {
        printTree(root, 0);
    } else {
        cout << "Tree is empty." << endl;
    }
    cout << "-------------------------" << endl;
}
//serialzition functions
template<typename T, typename S>
void BPlusTree<T, S>::serialize_Tree(){
    ofstream serilaize_file("DB_files/"+file_name+"serialize.txt");
    queue<Node*> visiting_queue;
    visiting_queue.push(root);
    while(!visiting_queue.empty()){
        Node* node=visiting_queue.front();
        string serialzed_node="";
        //keys|isLeaf| and then for leaf will be offset and for internal nodes there will be num_of_children 
        for(int i=0;i<node->keys.size()-1;i++){
            serialzed_node+=Type_to_String(node->keys[i])+" ";
        }
        serialzed_node+=Type_to_String(node->keys[node->keys.size()-1])+"|";
        if(node->isLeaf) {
            serialzed_node+="1|"+Type_to_String(node->offset);
        }
        else{ 
            serialzed_node+="0|";
            for(int i=0;i<node->children.size();i++){
                visiting_queue.push(node->children[i]);
            }
            serialzed_node+=Type_to_String<size_t>(node->children.size());
        }
        visiting_queue.pop();
        serilaize_file<<serialzed_node<<endl;
    }
    serilaize_file.close();
}
// Helper for static_assert false in templates
template<typename T, typename S>
void BPlusTree<T, S>::deserialize_Tree(){
    if(!filesystem::exists("DB_files/"+file_name+"serialize.txt")) return;
    ifstream serialized_file("DB_files/"+file_name+"serialize.txt");
    vector<Node*> nodes;
    vector<string> indices;
    string line;
    while(getline(serialized_file,line)){
        vector<string> tokens;
        std::stringstream ss(line);
        std::string token;
        while (std::getline(ss, token, '|')) {
            tokens.push_back(token);
        }
        Node* new_node=new Node();
        stringstream keys(tokens[0]);
        string key;
        while(std::getline(keys,key,' ')){
           new_node->keys.push_back(String_to_Type<T>(key)); //parse key as type T
        }
        if(tokens[1]=="1"){ //check for leaf 1 means true
            new_node->isLeaf=true;
            new_node->offset=stoi(tokens[2]);
        }
        else{
            new_node->isLeaf=false;
            indices.push_back(tokens[2]);
        }
        nodes.push_back(new_node);
    }
    serialized_file.close();
    int counter=1;
    for(int i=0;i<indices.size();i++){
        Node* curr=nodes[i];
        int num_of_childs=stoi(indices[i]);
        for(int x=0;x<num_of_childs;x++){
            curr->children.push_back(nodes[counter]);
            counter++;
        }
    }
    for(int i=indices.size();i<nodes.size()-1;i++){
        nodes[i]->next=nodes[i+1];
    }
    this->root=nodes[0];

}
//max min functions
template<typename T, typename S>
T BPlusTree<T, S>::get_Max(){
    Node* current=root;
    while(!current->isLeaf)
        current=current->children.back();
    return current->keys.back();
}
template<typename T, typename S>
T BPlusTree<T, S>::get_Min(){
   return findSmallestInSubtree(root);
}
//GC function implementation
template<typename T, typename S>
void BPlusTree<T, S>::GC_with_values(vector<S> values) {
    if (root == nullptr) return;
    Node* current = root;
    while (!current->isLeaf) {
        current = current->children[0];
    }
    int start_ind=0;
    while (current != nullptr) {
        int size=current->keys.size();
        vector<string> data;
        for(int i=0;i<size;i++){
            data.push_back((Type_to_String<S>(values[start_ind + i])));
        }
        current->offset=write_line_to_file(file_name+'1', data); //rewrite to new location
        start_ind += size;
        current = current->next;
    }
    filesystem::remove("DB_files/" + file_name + ".txt"); //delete old file
    filesystem::rename("DB_files/" + file_name + "1.txt", "DB_files/" + file_name + ".txt"); //rename new file to old file
}


// int main() {
//     // Degree t=3 means each node has [t-1, 2t-1] keys -> [2, 5] keys
//      if(filesystem::exists("DB_files")){
//                     filesystem::remove_all("DB_files");
//                }     
//                filesystem::create_directory("DB_files");
//     BPlusTree<int, int> tree(2);

//     tree.insert(8, 16);
//     tree.insert(20, 40);
//     tree.insert(5, 10);
//     tree.insert(6, 12);
//     tree.insert(12, 24);
//     tree.insert(30, 60);
//     tree.insert(7, 14);
//     tree.insert(17, 34);

//     cout << "B+ Tree after insertions:" << endl;
//     tree.printTree();

//     int searchKey = 12;
//     optional<int> foundValue = tree.search(searchKey);
//     cout << "\nSearching for key " << searchKey << ": ";
//     if (foundValue) {
//         cout << "Found value " << *foundValue << endl;
//     } else {
//         cout << "Not Found" << endl;
//     }

//     int searchKey2 = 99;
//     optional<int> foundValue2 = tree.search(searchKey2);
//     cout << "Searching for key " << searchKey2 << ": ";
//     if (foundValue2) {
//         cout << "Found value " << *foundValue2 << endl;
//     } else {
//         cout << "Not Found" << endl;
//     }
    
//     int lower = 6, upper = 20;
//     vector<pair<int, int>> rangeResult = tree.rangeQuery(lower, upper);
//     cout << "\nRange query [" << lower << ", " << upper << "]: ";
//     for (const auto& [k, v] : rangeResult) {
//         cout << "(" << k << ", " << v << ") ";
//     }
//     cout << endl;

//     vector<int> rangeResultb = tree.rangeQueryKeys(lower, upper);
//     cout << "\nRange query [" << lower << ", " << upper << "]: ";
//     for (const auto& k : rangeResultb) {
//         cout << "(" << k  << ") ";
//     }
//     cout << endl;
    
//     int removeKey = 5;
//     tree.remove(removeKey);
//     cout << "\nB+ Tree after removing " << removeKey << ":" << endl;
//     tree.printTree();

//     int removeKey2 = 6;
//     tree.remove(removeKey2);
//     cout << "\nB+ Tree after removing " << removeKey2 << ":" << endl;
//     tree.printTree();

//     vector<pair<int, int>> allValues = tree.getAllValues();
//     cout << "\nAll values in the B+ Tree: ";
//     for (const auto& [k, v] : allValues) {
//         cout << "(" << k << ", " << v << ") ";
//     }
//     vector<int> allValuesa = tree.getAllKeys();
//     cout << "\nAll keys in the B+ Tree: ";
//     for (const auto& k : allValuesa) {
//         cout << "(" << k << ") ";
//     }

//     cout << endl;
//     tree.GC_with_values(vector<int>{10, 14, 16, 24, 34, 40, 60});
//     cout << "\nTree after GC : ";
//     tree.printTree();
//     tree.serialize_Tree();
    

//     BPlusTree<int, int> tree_seconde(2);
//     tree_seconde.deserialize_Tree();
//     tree_seconde.printTree();
//     vector<pair<int, int>> dummy=tree_seconde.getAllValues();
//     for(const auto& p:dummy) cout<<p.first<<" "<<p.second<<endl;
//     return 0;
// }
#endif