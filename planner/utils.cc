#include "utils.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <functional>

namespace ToyDBMS {
	std::string table_name(std::string attribute_name){
		return {attribute_name.begin(), std::find(attribute_name.begin(), attribute_name.end(), '.')};
	}
}
