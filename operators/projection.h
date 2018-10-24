#pragma once

#include "operator.h"

namespace ToyDBMS {
	class Projection : public Operator {
		private:
			std::unique_ptr<Operator> child;
			std::shared_ptr<Header> header_ptr;

		public:
			Projection(std::unique_ptr<Operator> child, Header &&header)
				: child(std::move(child)),
				  header_ptr(std::make_shared<Header>(header)) {}

			const Header &header() override { return *header_ptr; }
			Row next() override;
			void reset() override { child->reset(); }
	};
}
