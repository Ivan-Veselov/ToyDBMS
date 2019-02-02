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
#include "../operators/EmptyOperator.h"
#include "../operators/unique.h"
#include "../operators/OptimizedUnique.h"
#include "../operators/cache.h"

#include "utils.h"
#include "joins_applier.h"
#include "rewriter.h"

namespace ToyDBMS {
struct PredicatesLists {
	std::vector<AttributePredicate*> joinPredicates;

	std::vector<ConstPredicate*> constFilterPredicates;

	std::vector<AttributePredicate*> attributesInequalityFilterPredicates;
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
					lists.constFilterPredicates.push_back(&dynamic_cast<ConstPredicate&>(pred));
					break;
				}

				case Predicate::Type::ATTR: {
					AttributePredicate& attributePredicate = dynamic_cast<AttributePredicate&>(pred);

					if (table_name(attributePredicate.left) == table_name(attributePredicate.right)) {
						if (attributePredicate.relation == Predicate::Relation::EQUAL) {
							throw std::runtime_error("Equality attribute filters are not yet supported");
						}

						lists.attributesInequalityFilterPredicates.push_back(&attributePredicate);
					} else {
						if (attributePredicate.relation != Predicate::Relation::EQUAL) {
							throw std::runtime_error("Inequality join predicates are not yet supported");
						}

						lists.joinPredicates.push_back(&attributePredicate);
					}

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

std::unordered_map<std::string, std::unique_ptr<Operator>> ConstructedQuery::processQueryOperators(
	const Query &query
) {
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
				ConstructedQuery constructedQuery(*fromQuery.query);

				tables[alias] = std::make_unique<AliasAppender>(
					std::move(constructedQuery.takeOperator()),
					alias
				);

				Table queryTable = constructedQuery.getCatalog().joinToOneTable(alias);
				catalog.tables.insert(std::make_pair(alias, queryTable));

				break;
			}

			default:
				throw std::runtime_error("Unsupported FROM part type");
		}
	}

	return std::move(tables);
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

void ConstructedQuery::createCatalog(const std::vector<std::string> &tablesNames) {
	std::vector<std::string> tablesToDelete;
	std::unordered_set<std::string> involvedTables(tablesNames.begin(), tablesNames.end());

	for (const auto &kv : catalog.tables) {
		if (involvedTables.find(kv.first) == involvedTables.end()) {
			tablesToDelete.push_back(kv.first);
		}
	}

	for (const std::string &tableName : tablesToDelete) {
		catalog.tables.erase(tableName);
	}
}

static void make_every_table_empty(std::unordered_map<std::string, std::unique_ptr<Operator>> &tables) {
	for (auto &kv : tables) {
		kv.second = std::make_unique<EmptyOperator>(kv.second->header());
	}
}

void ConstructedQuery::apply_const_filters(
	std::unordered_map<std::string, std::unique_ptr<Operator>> &tables,
	const std::vector<ConstPredicate*> &constFilterPredicates
) {
	ColumnsBounds bounds = rewrite_const_filters(constFilterPredicates);
	if (!bounds.isValid) {
		make_every_table_empty(tables);
		return;
	}


	std::vector<std::unique_ptr<ConstPredicate>> resultingConstFilterPredicates;
	for (const auto &kv : bounds.columnExactValue) {
		const Column &column = catalog.getColumn(kv.first);
		if (kv.second < column.min || column.max < kv.second) {
			make_every_table_empty(tables);
			return;
		}

		resultingConstFilterPredicates.push_back(std::make_unique<ConstPredicate>(
			kv.first, kv.second, Predicate::Relation::EQUAL
		));
	}

	for (const auto &kv : bounds.columnUpperBound) {
		const Column &column = catalog.getColumn(kv.first);
		if (kv.second <= column.min) {
			make_every_table_empty(tables);
			return;
		}

		if (kv.second > column.max) {
			continue;
		}

		resultingConstFilterPredicates.push_back(std::make_unique<ConstPredicate>(
			kv.first, kv.second, Predicate::Relation::LESS
		));
	}

	for (const auto &kv : bounds.columnLowerBound) {
		const Column &column = catalog.getColumn(kv.first);
		if (kv.second >= column.max) {
			make_every_table_empty(tables);
			return;
		}

		if (kv.second < column.min) {
			continue;
		}

		resultingConstFilterPredicates.push_back(std::make_unique<ConstPredicate>(
			kv.first, kv.second, Predicate::Relation::GREATER
		));
	}

	for (const std::unique_ptr<ConstPredicate> &predicate : resultingConstFilterPredicates) {
		std::string tableName = table_name(predicate->attribute);
		std::unique_ptr<Predicate> predicateCopy = std::make_unique<ConstPredicate>(*predicate);

		auto it = tables.find(tableName);
		if (it == tables.end()) {
			throw std::runtime_error("Unknown table: " + tableName);
		}

		tables[tableName] = std::make_unique<Filter>(
			std::move(tables[tableName]), std::move(predicateCopy)
		);
	}
}

void ConstructedQuery::apply_attribute_inequality_filters(
	std::unordered_map<std::string, std::unique_ptr<Operator>> &tables,
	const std::vector<AttributePredicate*> &inequalityPredicates
) {
	AttributeInequalityFilters rewriterResult =
		AttributeInequalitiesRewriter(inequalityPredicates).rewrite();

	if (!rewriterResult.isValid) {
		make_every_table_empty(tables);
		return;
	}

	for (AttributePredicate *predicate : rewriterResult.predicates) {
		std::string tableName = table_name(predicate->left);
		std::unique_ptr<Predicate> predicateCopy = std::make_unique<AttributePredicate>(*predicate);

		auto it = tables.find(tableName);
		if (it == tables.end()) {
			throw std::runtime_error("Unknown table: " + tableName);
		}

		tables[tableName] = std::make_unique<Filter>(
			std::move(tables[tableName]), std::move(predicateCopy)
		);
	}
}

std::unordered_set<std::string> ConstructedQuery::getAttributesInResult(const Query &query) {
	std::unordered_set<std::string> attributesInProjection;

	switch (query.selection.type) {
		case ToyDBMS::SelectionClause::Type::ALL: {
			for (const auto &kv1 : catalog.tables) {
				for (const auto &kv2 : kv1.second.columns) {
					attributesInProjection.insert(kv1.first + "." + kv2.first);
				}
			}

			break;
		}

		case ToyDBMS::SelectionClause::Type::LIST: {
			for (const SelectionPart &attr : query.selection.attrs) {
				if (attr.function != ToyDBMS::SelectionPart::AggregateFunction::NONE) {
					throw std::runtime_error("Unsupported aggregation function");
				}

				attributesInProjection.insert(attr.attribute);
			}

			break;
		}

		default:
			throw std::runtime_error("Unsupported selection clause");
	}

	return attributesInProjection;
}

std::vector<std::string> ConstructedQuery::getOrderedAttributes(const Query &query) {
	std::unordered_set<std::string> attributesInResult = getAttributesInResult(query);

	std::vector<std::string> orderedAttributes;
	for (const auto &kv1 : catalog.tables) {
		for (const auto &kv2 : kv1.second.columns) {
			if (kv2.second.order != Column::SortOrder::ASC && kv2.second.order != Column::SortOrder::DESC) {
				continue;
			}

			std::string attributeName = kv1.first + "." + kv2.first;

			if (attributesInResult.find(attributeName) == attributesInResult.end()) {
				continue;
			}

			orderedAttributes.push_back(attributeName);
		}
	}

	return std::move(orderedAttributes);
}

std::vector<std::string> ConstructedQuery::getUniqueAttributes(const Query &query) {
	std::unordered_set<std::string> attributesInResult = getAttributesInResult(query);

	std::vector<std::string> uniqueAttributes;
	for (const auto &kv1 : catalog.tables) {
		for (const auto &kv2 : kv1.second.columns) {
			if (!kv2.second.unique) {
				continue;
			}

			std::string attributeName = kv1.first + "." + kv2.first;

			if (attributesInResult.find(attributeName) == attributesInResult.end()) {
				continue;
			}

			uniqueAttributes.push_back(attributeName);
		}
	}

	return std::move(uniqueAttributes);
}

std::string chooseTableWithMaxNumOfAttributes(const std::vector<std::string> &attributes) {
	std::unordered_map<std::string, int> attributesFromTable;
	for (const std::string &attribute : attributes) {
		attributesFromTable[table_name(attribute)]++;
	}

	int maxAttributes = -1;
	std::string table;

	for (const auto &kv : attributesFromTable) {
		if (kv.second > maxAttributes) {
			maxAttributes = kv.second;
			table = kv.first;
		}
	}

	return table;
}

ConstructedQuery::ConstructedQuery(const Query &query) {
	std::vector<std::string> tablesNames = getTablesNames(query);
	createCatalog(tablesNames);

	std::vector<std::string> orderedAttributes = getOrderedAttributes(query);
	std::vector<std::string> uniqueAttributes = getUniqueAttributes(query);

	std::unordered_map<std::string, std::unique_ptr<Operator>> tables = processQueryOperators(query);
	PredicatesLists predicatesLists = createPredicatesLists(query);

	apply_const_filters(tables, predicatesLists.constFilterPredicates);
	apply_attribute_inequality_filters(tables, predicatesLists.attributesInequalityFilterPredicates);

	bool isOrdered = false;
	std::string orderedTable = chooseTableWithMaxNumOfAttributes(orderedAttributes);
	std::vector<JoinApplicationResult> isolatedTables;

	if (orderedAttributes.size() == 0) {
		isolatedTables = JoinsApplier(tables, predicatesLists.joinPredicates, catalog).applyJoins();
	} else {
		isOrdered = true;
		isolatedTables = JoinsApplier(tables, predicatesLists.joinPredicates, catalog).applyJoins(orderedTable);
	}

	resultingOperator = std::move(isolatedTables[0].op);
	for (size_t i = 1; i < isolatedTables.size(); i++) {
		std::unique_ptr<Operator> rightOperator = std::move(isolatedTables[i].op);
		if (isolatedTables[i].wasJoin) {
			rightOperator = std::make_unique<Cache>(std::move(rightOperator));
		}

		resultingOperator = std::make_unique<CrossJoin>(
			std::move(resultingOperator), std::move(rightOperator)
		);
	}

	switch (query.selection.type) {
		case ToyDBMS::SelectionClause::Type::ALL: {
			resultingOperator = wrap_in_default_projection(std::move(resultingOperator), tablesNames);
			break;
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

			resultingOperator = std::make_unique<Projection>(std::move(resultingOperator), std::move(header));
			break;
		}

		default:
			throw std::runtime_error("Unsupported selection clause");
	}

	bool failingTest = true;

	if (!query.distinct) {
		failingTest = false;
	}

	if (tablesNames.size() == 1) {
		failingTest = false;
	}

	if (!isOrdered) {
		failingTest = false;
	}

	if (isolatedTables.size() == 1) {
		failingTest = false;
	}

	/*
	if (getAttributesInResult(query).size() < 15) {
		failingTest = false;
	}

	if (orderedAttributes.size() < 10) {
		failingTest = false;
	}*/

	if (query.distinct) {
		if (tablesNames.size() == 1 && uniqueAttributes.size() > 0) {
			return;
		}

		if (isOrdered) {
			std::vector<std::string> attributes;

			if (tablesNames.size() == 1) {
				attributes = orderedAttributes;
			} else {
				for (std::string &attribute : orderedAttributes) {
					if (table_name(attribute) == orderedTable) {
						attributes.push_back(attribute);
					}
				}
			}

			resultingOperator = std::make_unique<OptimizedUnique>(std::move(resultingOperator), attributes);
		} else {
			resultingOperator = std::make_unique<Unique>(std::move(resultingOperator));
		}
	}

	/*if (failingTest) {
		throw std::runtime_error("!!!");
	}*/
}

const Catalog& ConstructedQuery::getCatalog() {
	return catalog;
}

std::unique_ptr<Operator> ConstructedQuery::takeOperator() {
	return std::move(resultingOperator);
}

}
