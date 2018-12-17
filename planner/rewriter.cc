#include "rewriter.h"

namespace ToyDBMS {
	ColumnsBounds rewrite_const_filters(const std::vector<ConstPredicate*> &constFilterPredicates) {
		ColumnsBounds result;

		for (ConstPredicate *predicate : constFilterPredicates) {
			const std::string &attribute = predicate->attribute;
			const Value &value = predicate->value;

			switch (predicate->relation) {
				case Predicate::Relation::EQUAL: {
					auto it1 = result.columnUpperBound.find(attribute);
					if (it1 != result.columnUpperBound.end()) {
						if (it1->second <= value) {
							result.isValid = false;
							return result;
						} else {
							result.columnUpperBound.erase(it1);
						}
					}

					auto it2 = result.columnLowerBound.find(attribute);
					if (it2 != result.columnLowerBound.end()) {
						if (it2->second >= value) {
							result.isValid = false;
							return result;
						} else {
							result.columnLowerBound.erase(it2);
						}
					}

					auto it3 = result.columnExactValue.find(attribute);
					if (it3 != result.columnExactValue.end()) {
						if (it3->second != value) {
							result.isValid = false;
							return result;
						}
					} else {
						result.columnExactValue.insert(std::make_pair(attribute, value));
					}

					break;
				}

				case Predicate::Relation::LESS: {
					auto it1 = result.columnExactValue.find(attribute);
					if (it1 != result.columnExactValue.end()) {
						if (it1->second >= value) {
							result.isValid = false;
							return result;
						} else {
							break;
						}
					}

					auto it2 = result.columnLowerBound.find(attribute);
					if (it2 != result.columnLowerBound.end()) {
						if (it2->second >= value) {
							result.isValid = false;
							return result;
						}
					}

					auto it3 = result.columnUpperBound.find(attribute);
					if (it3 == result.columnUpperBound.end() || it3->second > value) {
						result.columnUpperBound.erase(attribute);
						result.columnUpperBound.insert(std::make_pair(attribute, value));
					}

					break;
				}

				case Predicate::Relation::GREATER: {
					auto it1 = result.columnExactValue.find(attribute);
					if (it1 != result.columnExactValue.end()) {
						if (it1->second <= value) {
							result.isValid = false;
							return result;
						} else {
							break;
						}
					}

					auto it2 = result.columnUpperBound.find(attribute);
					if (it2 != result.columnUpperBound.end()) {
						if (it2->second <= value) {
							result.isValid = false;
							return result;
						}
					}

					auto it3 = result.columnLowerBound.find(attribute);
					if (it3 == result.columnLowerBound.end() || it3->second < value) {
						result.columnLowerBound.erase(attribute);
						result.columnLowerBound.insert(std::make_pair(attribute, value));
					}

					break;
				}

				default:
					throw std::runtime_error("Unsupported predicate relation");
			}
		}

		return result;
	}

	AttributeInequalitiesRewriter::AttributeInequalitiesRewriter(
		const std::vector<AttributePredicate*> &inequalityPredicates
	) : inequalityPredicates(inequalityPredicates) {
		for (AttributePredicate* predicate : inequalityPredicates) {
			registerAttribute(predicate->left);
			registerAttribute(predicate->right);
		}
	}

	AttributeInequalityFilters AttributeInequalitiesRewriter::rewrite() {
		AttributeInequalityFilters result;

		for (AttributePredicate* predicate : inequalityPredicates) {
			std::string less = predicate->left;
			std::string greater = predicate->right;

			if (less == greater) {
				result.isValid = false;
				return result;
			}

			if (predicate->relation == ToyDBMS::Predicate::Relation::GREATER) {
				std::swap(less, greater);
			}

			if (setOfGreater[less].count(greater)) {
				continue;
			}

			if (setOfGreater[greater].count(less)) {
				result.isValid = false;
				return result;
			}

			result.predicates.push_back(predicate);
			addInequality(less, greater);
		}

		return result;
	}

	void AttributeInequalitiesRewriter::registerAttribute(const std::string &name) {
		if (setOfGreater.count(name)) {
			return;
		}

		setOfGreater.insert(
			std::make_pair<std::string, std::unordered_set<std::string>>(
				std::string(name),
				std::unordered_set<std::string>()
			)
		);
	}

	void AttributeInequalitiesRewriter::addInequality(const std::string &less, const std::string &greater) {
		if (setOfGreater[less].count(greater)) {
			return;
		}

		setOfGreater[less].insert(greater);
		for (const std::string &evenGreater : setOfGreater[greater]) {
			addInequality(less, evenGreater);
		}

		for (const auto &kv : setOfGreater) {
			if (kv.second.count(less)) {
				addInequality(kv.first, greater);
			}
		}
	}
}
