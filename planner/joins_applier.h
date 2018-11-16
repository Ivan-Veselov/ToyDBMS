#pragma once
#include <memory>
#include "../parser/query.h"
#include "../operators/operator.h"
#include "../operators/filter.h"
#include "../operators/join.h"

#include <unordered_set>
#include <unordered_map>

namespace ToyDBMS {
	class JoinsApplier {
		private:
			std::unordered_map<std::string, std::unique_ptr<Operator>> &tables;
			const std::vector<AttributePredicate*> &joinPredicates;

		public:
			JoinsApplier(
				std::unordered_map<std::string, std::unique_ptr<Operator>> &tables,
				const std::vector<AttributePredicate*> &joinPredicates
			) : tables(tables), joinPredicates(joinPredicates) {
			}

			std::vector<std::unique_ptr<Operator>> applyJoins();
	};
}
