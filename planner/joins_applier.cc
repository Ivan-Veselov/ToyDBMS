#include "joins_applier.h"

#include "utils.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <functional>

namespace ToyDBMS {

std::vector<std::unique_ptr<Operator>> JoinsApplier::applyJoins() {
	std::unordered_set<std::string> usedTables(tables.size());
	std::vector<bool> usedPredicates(joinPredicates.size(), false);

	std::vector<std::unique_ptr<Operator>> isolatedTables;

	for (std::pair<const std::string, std::unique_ptr<Operator>> &table : tables) {
		if (usedTables.find(table.first) != usedTables.end()) {
			continue;
		}

		usedTables.insert(table.first);
		std::unique_ptr<Operator> currentRelation = std::move(table.second);

		bool wasUpdated;
		do {
			wasUpdated = false;

			for (size_t i = 0; i < joinPredicates.size(); i++) {
				if (usedPredicates[i]) {
					continue;
				}

				std::string leftAttribute = joinPredicates[i]->left;
				std::string rightAttribute = joinPredicates[i]->right;
				std::string leftTable = table_name(leftAttribute);
				std::string rightTable = table_name(rightAttribute);

				if (
					usedTables.find(leftTable) == usedTables.end() &&
					usedTables.find(rightTable) == usedTables.end()
				) {
					continue;
				}

				if (usedTables.find(leftTable) == usedTables.end()) {
					std::swap(leftTable, rightTable);
					std::swap(leftAttribute, rightAttribute);
				}

				currentRelation = std::make_unique<NLJoin>(
					std::move(currentRelation), std::move(tables[rightTable]),
					leftAttribute, rightAttribute
				);
				usedTables.insert(rightTable);
				usedPredicates[i] = true;
				wasUpdated = true;
				break;
			}

			for (size_t i = 0; i < joinPredicates.size(); i++) {
				if (usedPredicates[i]) {
					continue;
				}

				std::string leftAttribute = joinPredicates[i]->left;
				std::string rightAttribute = joinPredicates[i]->right;
				std::string leftTable = table_name(leftAttribute);
				std::string rightTable = table_name(rightAttribute);

				if (
					usedTables.find(leftTable) != usedTables.end() &&
					usedTables.find(rightTable) != usedTables.end()
				) {
					currentRelation = std::make_unique<Filter>(
						std::move(currentRelation),
						std::move(std::make_unique<AttributePredicate>(*joinPredicates[i]))
					);

					usedPredicates[i] = true;
				}
			}
		} while (wasUpdated);

		isolatedTables.push_back(std::move(currentRelation));
	}

	return isolatedTables;
}

}

