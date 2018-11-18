#include <stdexcept>
#include "parser/query.h"
#include "planner/constructor.h"
#include "operators/print.h"

using namespace ToyDBMS;

int main(){
    try {
    	std::cout.sync_with_stdio(false);
        Print p {ConstructedQuery(Query::parse(std::cin)).takeOperator()};
        while(p.next());
        return 0;
    } catch(std::exception &e){
        std::cerr << e.what();
        return 1;
    }
}
