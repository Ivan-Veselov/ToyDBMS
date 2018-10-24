#pragma once

#include "operator.h"

namespace ToyDBMS {
	class AliasAppender : public Operator {
		private:
			std::unique_ptr<Operator> child;
			std::shared_ptr<Header> header_ptr;

			Header construct_header(const Header &h, const std::string &alias) {
				Header res;
				res.reserve(h.size());

				for (const std::string &attribute : h) {
					res.push_back(alias + "." + attribute);
				}

				return res;
			}

		public:
			AliasAppender(std::unique_ptr<Operator> child, const std::string &alias)
				: child(std::move(child)),
				  header_ptr(std::make_shared<Header>(construct_header(this->child->header(), alias))) {}

			const Header &header() override { return *header_ptr; }
			Row next() override { return child->next(); }
			void reset() override { child->reset(); }
	};
}
