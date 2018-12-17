#pragma once
#include "operator.h"
#include "../parser/query.h"

#include <unordered_set>

namespace ToyDBMS {
	class Unique : public Operator {
		std::unique_ptr<Operator> child;
		std::unordered_set<Row, RowHasher> hashTable;

		public:
			Unique(std::unique_ptr<Operator> child)
			: child(std::move(child)) {}

			const Header &header() { return child->header(); }

			Row next() override;

			void reset() override {
				child->reset();
				hashTable.clear();
			}
	};
}
