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
	struct JoinApplicationResult {
		bool wasJoin;

		std::unique_ptr<Operator> op;

		JoinApplicationResult(bool wasJoin, std::unique_ptr<Operator> op)
		: wasJoin(wasJoin), op(std::move(op)) {
		}
	};

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

			std::vector<JoinApplicationResult> applyJoins();

			std::vector<JoinApplicationResult> applyJoins(const std::string &firstTable);

		private:
			JoinApplicationResult processTable(std::pair<const std::string, std::unique_ptr<Operator>> &table);

			int findNextJoinPredicate();
	};
}
