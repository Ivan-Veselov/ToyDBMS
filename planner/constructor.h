#pragma once
#include <memory>
#include "../parser/query.h"
#include "../operators/operator.h"

namespace ToyDBMS {
	class ConstructedQuery {
		private:
			std::unique_ptr<Operator> resultingOperator;

		public:
			ConstructedQuery(const Query &query);

			std::unique_ptr<Operator> takeOperator();
	};
}
