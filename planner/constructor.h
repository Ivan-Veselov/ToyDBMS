#pragma once
#include <memory>
#include <unordered_set>
#include "../parser/query.h"
#include "../operators/operator.h"
#include "catalog.h"

namespace ToyDBMS {
	class ConstructedQuery {
		private:
			std::unique_ptr<Operator> resultingOperator;
			Catalog catalog;

		public:
			ConstructedQuery(const Query &query);

			std::unique_ptr<Operator> takeOperator();

			const Catalog& getCatalog();

		private:
			void createCatalog(const std::vector<std::string> &tablesNames);
			std::unordered_map<std::string, std::unique_ptr<Operator>> processQueryOperators(
				const Query &query
			);

			void apply_const_filters(
				std::unordered_map<std::string, std::unique_ptr<Operator>> &tables,
				const std::vector<ConstPredicate*> &constFilterPredicates
			);

			void apply_attribute_inequality_filters(
				std::unordered_map<std::string, std::unique_ptr<Operator>> &tables,
				const std::vector<AttributePredicate*> &inequalityPredicates
			);

			std::vector<std::string> getOrderedAttributes(const Query &query);

			std::vector<std::string> getUniqueAttributes(const Query &query);

			std::unordered_set<std::string> getAttributesInResult(const Query &query);
	};
}
