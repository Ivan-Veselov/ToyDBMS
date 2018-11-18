#pragma once

#include <memory>
#include <unordered_map>

#include "../parser/query.h"
#include "../operators/operator.h"

namespace ToyDBMS {
	struct ColumnsBounds {
		bool isValid = true;

		std::unordered_map<std::string, Value> columnUpperBound;
		std::unordered_map<std::string, Value> columnLowerBound;
		std::unordered_map<std::string, Value> columnExactValue;
	};

	ColumnsBounds rewrite_const_filters(const std::vector<ConstPredicate*> &constFilterPredicates);
}
