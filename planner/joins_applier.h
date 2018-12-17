#pragma once
#include <memory>
#include "../parser/query.h"
#include "../operators/operator.h"
#include "../operators/filter.h"
#include "../operators/join.h"

#include "catalog.h"

#include <unordered_set>
#include <unordered_map>

namespace ToyDBMS {
	class JoinsApplier {
		private:
			std::unordered_map<std::string, std::unique_ptr<Operator>> &tables;
			const std::vector<AttributePredicate*> &joinPredicates;
			const Catalog &catalog;

			std::unordered_set<std::string> usedTables;
			std::vector<bool> usedPredicates;

		public:
			JoinsApplier(
				std::unordered_map<std::string, std::unique_ptr<Operator>> &tables,
				const std::vector<AttributePredicate*> &joinPredicates,
				const Catalog &catalog
			) : tables(tables),
				joinPredicates(joinPredicates),
				catalog(catalog),
				usedTables(tables.size()),
				usedPredicates(joinPredicates.size(), false) {
			}

			std::vector<std::unique_ptr<Operator>> applyJoins();

			std::vector<std::unique_ptr<Operator>> applyJoins(const std::string &firstTable);

		private:
			std::unique_ptr<Operator> processTable(std::pair<const std::string, std::unique_ptr<Operator>> &table);

			int findNextJoinPredicate();
	};
}
