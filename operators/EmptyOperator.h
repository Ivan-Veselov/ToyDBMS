#pragma once
#include <memory>
#include "operator.h"

namespace ToyDBMS {
	class EmptyOperator : public Operator {
		private:
			std::shared_ptr<Header> header_ptr;

		public:
			EmptyOperator(const Header &header)
				: header_ptr(std::make_shared<Header>(header)) {}

			const Header &header() override { return *header_ptr; }
			Row next() override { return {}; }
			void reset() override {}
	};
}
