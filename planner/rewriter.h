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

	struct AttributeInequalityFilters {
		bool isValid = true;

		std::vector<AttributePredicate*> predicates;
	};

	ColumnsBounds rewrite_const_filters(const std::vector<ConstPredicate*> &constFilterPredicates);

	class AttributeInequalitiesRewriter {
		const std::vector<AttributePredicate*> inequalityPredicates;

		public:
			AttributeInequalitiesRewriter(const std::vector<AttributePredicate*> &inequalityPredicates)
			: inequalityPredicates(inequalityPredicates) {}

			AttributeInequalityFilters rewrite();
	};
}
