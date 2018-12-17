#pragma once
#include "operator.h"
#include "../parser/query.h"

#include <unordered_set>

namespace ToyDBMS {
	class OptimizedUnique : public Operator {
		std::unique_ptr<Operator> child;
		std::unordered_set<Row, RowHasher> hashTable;

		const std::string orderedAttribute;

		public:
			OptimizedUnique(std::unique_ptr<Operator> child, const std::string &orderedAttribute)
			: child(std::move(child)), orderedAttribute(orderedAttribute) {}

			const Header &header() { return child->header(); }

			Row next() override;

			void reset() override {
				child->reset();
				hashTable.clear();
			}
	};
}
