#pragma once
#include "operator.h"
#include "../parser/query.h"

#include <unordered_set>

namespace ToyDBMS {
	class Cache : public Operator {
		std::unique_ptr<Operator> child;

		std::vector<Row> cached;
		std::vector<Row>::iterator iterator;

		public:
			Cache(std::unique_ptr<Operator> child)
			: child(std::move(child)) {
				Row current_row = this->child->next();
				while (current_row) {
					cached.push_back(current_row);
					current_row = this->child->next();
				}

				iterator = cached.begin();
			}

			const Header &header() { return child->header(); }

			Row next() override;

			void reset() override;
	};
}
