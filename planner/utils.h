#pragma once
#include <memory>
#include "../parser/query.h"
#include "../operators/operator.h"

namespace ToyDBMS {
	std::string table_name(std::string attribute_name);
}
