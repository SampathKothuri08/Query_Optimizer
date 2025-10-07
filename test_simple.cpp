#include <iostream>
#include "tokenizer.h"

int main() {
    std::string sql = "SELECT name FROM users";
    Tokenizer tokenizer(sql);
    auto tokens = tokenizer.tokenize();
    
    std::cout << "Success! Got " << tokens.size() << " tokens" << std::endl;
    return 0;
}