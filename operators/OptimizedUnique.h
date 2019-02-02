#pragma once
#include "operator.h"
#include "../parser/query.h"

#include <unordered_set>

namespace ToyDBMS {
	class OptimizedUnique : public Operator {
		std::unique_ptr<Operator> child;
		std::unordered_set<std::vector<Value>> hashTable;

		const std::vector<std::string> orderedAttributes;
		std::vector<int> indicesOfNotOrdered;

		std::vector<Value> attributeValue;

		public:
			OptimizedUnique(
				std::unique_ptr<Operator> child,
				const std::vector<std::string> &orderedAttributes
			) : child(std::move(child)), orderedAttributes(orderedAttributes) {
				std::unordered_set<int> indicesOfOrdered;
				for (const std::string &attribute : orderedAttributes) {
					indicesOfOrdered.insert(this->child->header().index(attribute));
				}

				for (int i = 0; i < this->child->header().size(); ++i) {
					if (indicesOfOrdered.find(i) == indicesOfOrdered.end()) {
						indicesOfNotOrdered.push_back(i);
					}
				}
			}

			const Header &header() { return child->header(); }

			Row next() override;

			void reset() override {
				child->reset();
				hashTable.clear();
				attributeValue.clear();
			}

		private:
			std::vector<Value> compress(const Row &row);
	};
}
