#include "joins_applier.h"

#include "utils.h"

#include <algorithm>
#include <sstream>
#include <stdexcept>
#include <functional>

namespace ToyDBMS {

std::vector<std::unique_ptr<Operator>> JoinsApplier::applyJoins() {
	return applyJoins(tables.begin()->first);
}

std::vector<std::unique_ptr<Operator>> JoinsApplier::applyJoins(const std::string &firstTable) {
	std::vector<std::unique_ptr<Operator>> isolatedTables;

	auto it = tables.find(firstTable);
	if (it == tables.end()) {
		throw std::runtime_error("Unknown table!");
	}

	isolatedTables.push_back(std::move(processTable(*it)));

	for (std::pair<const std::string, std::unique_ptr<Operator>> &table : tables) {
		if (usedTables.find(table.first) != usedTables.end()) {
			continue;
		}

		isolatedTables.push_back(std::move(processTable(table)));
	}

	return isolatedTables;
}

std::unique_ptr<Operator> JoinsApplier::processTable(std::pair<const std::string, std::unique_ptr<Operator>> &table) {
	usedTables.insert(table.first);
	std::unique_ptr<Operator> currentRelation = std::move(table.second);

	while (true) {
		int i = findNextJoinPredicate();
		if (i == -1) {
			break;
		}

		std::string leftAttribute = joinPredicates[i]->left;
		std::string rightAttribute = joinPredicates[i]->right;
		std::string leftTable = table_name(leftAttribute);
		std::string rightTable = table_name(rightAttribute);

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
	}

	return std::move(currentRelation);
}

int JoinsApplier::findNextJoinPredicate() {
	for (size_t i = 0; i < joinPredicates.size(); i++) {
		if (usedPredicates[i]) {
			continue;
		}

		std::string leftAttribute = joinPredicates[i]->left;
		std::string rightAttribute = joinPredicates[i]->right;
		std::string leftTable = table_name(leftAttribute);
		std::string rightTable = table_name(rightAttribute);

		if (!catalog.getColumn(leftAttribute).unique && !catalog.getColumn(rightAttribute).unique) {
			continue;
		}

		if (
			usedTables.find(leftTable) == usedTables.end() &&
			usedTables.find(rightTable) == usedTables.end()
		) {
			continue;
		}

		return i;
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
			usedTables.find(leftTable) == usedTables.end() &&
			usedTables.find(rightTable) == usedTables.end()
		) {
			continue;
		}

		return i;
	}

	return -1;
}

}

