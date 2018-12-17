#pragma once

#include <memory>
#include <unordered_set>
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

		std::unordered_map<std::string, std::unordered_set<std::string>> setOfGreater;

		public:
			AttributeInequalitiesRewriter(const std::vector<AttributePredicate*> &inequalityPredicates);

			AttributeInequalityFilters rewrite();

		private:
			void registerAttribute(const std::string &name);

			void addInequality(const std::string &less, const std::string &greater);
	};
}
