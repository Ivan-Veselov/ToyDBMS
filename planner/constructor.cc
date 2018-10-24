#include <algorithm>
#include <functional>
#include <iterator>
#include <stdexcept>
#include <unordered_set>
#include <unordered_map>
#include "constructor.h"

#include "../operators/datasource.h"
#include "../operators/filter.h"
#include "../operators/join.h"
#include "../operators/projection.h"
#include "../operators/aliasappender.h"

namespace ToyDBMS {

static std::string table_name(std::string attribute_name){
    return {attribute_name.begin(), std::find(attribute_name.begin(), attribute_name.end(), '.')};
}

struct PredicatesLists {
	std::vector<AttributePredicate*> joinPredicates;

	std::vector<ConstPredicate*> filterPredicates;
};

static void for_each_simple_predicate(Predicate &predicate, std::function<void(Predicate&)> action) {
    switch(predicate.type) {
		case Predicate::Type::CONST:
		case Predicate::Type::ATTR:
			action(predicate);
			break;

		case Predicate::Type::AND: {
			const auto &pred = dynamic_cast<const ANDPredicate&>(predicate);
			for_each_simple_predicate(*pred.left, action);
			for_each_simple_predicate(*pred.right, action);
			break;
		}

		case Predicate::Type::INQUERY:
		case Predicate::Type::OR:
			throw std::runtime_error("OR and INQUERY predicates are not yet supported");

		default:
			throw std::runtime_error("encountered a predicate not yet supported");
    }
}

static const PredicatesLists createPredicatesLists(const Query &query) {
	PredicatesLists lists;

	if (query.where == nullptr) {
		return lists;
	}

	Predicate& predicate = *query.where;

	for_each_simple_predicate(
		predicate,
		[&lists](auto &pred) {
			switch(pred.type) {
				case Predicate::Type::CONST: {
					lists.filterPredicates.push_back(&dynamic_cast<ConstPredicate&>(pred));
					break;
				}

				case Predicate::Type::ATTR: {
					AttributePredicate& attributePredicate = dynamic_cast<AttributePredicate&>(pred);

					if (attributePredicate.relation != Predicate::Relation::EQUAL) {
						throw std::runtime_error("Inequality attribute predicates are not yet supported");
					}

					lists.joinPredicates.push_back(&attributePredicate);
					break;
				}

				case Predicate::Type::AND:
					throw std::runtime_error("Missed AND predicate detected");

				case Predicate::Type::INQUERY:
				case Predicate::Type::OR:
					throw std::runtime_error("OR and INQUERY predicates are not yet supported");

				default:
					throw std::runtime_error("encountered a predicate not yet supported");
		    }
		}
	);

	return lists;
}

static std::vector<std::string> getTablesNames(const Query &query) {
	std::vector<std::string> tables;
	tables.reserve(query.from.size());

	for (const std::unique_ptr<FromPart> &fromPart : query.from) {
		switch (fromPart->type) {
			case FromPart::Type::TABLE: {
				FromTable& fromTable = dynamic_cast<FromTable&>(*fromPart);

				tables.push_back(fromTable.table_name);
				break;
			}

			case FromPart::Type::QUERY: {
				FromQuery& fromQuery = dynamic_cast<FromQuery&>(*fromPart);

				tables.push_back(fromQuery.alias);
				break;
			}

			default:
				throw std::runtime_error("Unsupported FROM part type");
		}
	}

	return std::move(tables);
}

static std::unordered_map<std::string, std::unique_ptr<Operator>> getQueryOperators(const Query &query) {
	std::unordered_map<std::string, std::unique_ptr<Operator>> tables(query.from.size());

	for (const std::unique_ptr<FromPart> &fromPart : query.from) {
		switch (fromPart->type) {
			case FromPart::Type::TABLE: {
				FromTable& fromTable = dynamic_cast<FromTable&>(*fromPart);

				std::string table_name = fromTable.table_name;
				tables[table_name] = std::make_unique<DataSource>("tables/" + table_name + ".csv");
				break;
			}

			case FromPart::Type::QUERY: {
				FromQuery& fromQuery = dynamic_cast<FromQuery&>(*fromPart);

				std::string alias = fromQuery.alias;
				tables[alias] = std::make_unique<AliasAppender>(
					std::move(create_plan(*fromQuery.query)),
					alias
				);

				break;
			}

			default:
				throw std::runtime_error("Unsupported FROM part type");
		}
	}

	return std::move(tables);
}

static std::vector<std::unique_ptr<Operator>> applyJoins(
	std::unordered_map<std::string, std::unique_ptr<Operator>> &tables,
	const std::vector<AttributePredicate*> &joinPredicates
) {
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

				if (
					usedTables.find(leftTable) != usedTables.end() &&
					usedTables.find(rightTable) != usedTables.end()
				) {
					throw std::runtime_error("Cyclic dependencies in joins are not supported");
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
			}
		} while (wasUpdated);

		isolatedTables.push_back(std::move(currentRelation));
	}

	return isolatedTables;
}

static std::unique_ptr<Operator> wrap_in_default_projection(
	std::unique_ptr<Operator> op,
	const std::vector<std::string> &tablesNames
) {
	Header defaultHeader = op->header();

	stable_sort(defaultHeader.begin(), defaultHeader.end(),
	    [&tablesNames](const std::string &a, const std::string &b) {
			std::string aTableName = table_name(a);
			std::string bTableName = table_name(b);

			if (aTableName == bTableName) {
				return false;
			}

			for (size_t i = 0; i < tablesNames.size(); i++) {
				if (tablesNames[i] == aTableName) {
					return true;
				}

				if (tablesNames[i] == bTableName) {
					return false;
				}
			}

	        throw std::runtime_error("unknown attributes " + a + " " + b);
	    }
	);

	return std::make_unique<Projection>(std::move(op), std::move(defaultHeader));
}

std::unique_ptr<Operator> create_plan(const Query &query) {
	std::unordered_map<std::string, std::unique_ptr<Operator>> tables = getQueryOperators(query);

	PredicatesLists predicatesLists = createPredicatesLists(query);

	for (ConstPredicate *predicate : predicatesLists.filterPredicates) {
		std::string tableName = table_name(predicate->attribute);

		auto it = tables.find(tableName);
		if (it == tables.end()) {
			throw std::runtime_error("Unknown table: " + tableName);
		}

		tables[tableName] = std::make_unique<Filter>(
			std::move(tables[tableName]), std::move(std::make_unique<ConstPredicate>(*predicate))
		);
	}

	std::vector<std::unique_ptr<Operator>> isolatedTables = applyJoins(
		tables,
		predicatesLists.joinPredicates
	);

	std::unique_ptr<Operator> resultingOperator = std::move(isolatedTables[0]);
	for (size_t i = 1; i < isolatedTables.size(); i++) {
		resultingOperator = std::make_unique<CrossJoin>(
			std::move(resultingOperator), std::move(isolatedTables[i])
		);
	}

	switch (query.selection.type) {
		case ToyDBMS::SelectionClause::Type::ALL: {
			return wrap_in_default_projection(std::move(resultingOperator), getTablesNames(query));
		}

		case ToyDBMS::SelectionClause::Type::LIST: {
			Header header;
			header.reserve(query.selection.attrs.size());

			for (const SelectionPart &attr : query.selection.attrs) {
				if (attr.function != ToyDBMS::SelectionPart::AggregateFunction::NONE) {
					throw std::runtime_error("Unsupported aggregation function");
				}

				header.push_back(attr.attribute);
			}

			return std::make_unique<Projection>(std::move(resultingOperator), std::move(header));
		}

		default:
			throw std::runtime_error("Unsupported selection clause");
	}
}

}
