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

	AttributeInequalityFilters AttributeInequalitiesRewriter::rewrite() {
		AttributeInequalityFilters result;

		result.predicates = inequalityPredicates;
		return result;
	}
}
