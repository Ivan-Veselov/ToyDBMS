#pragma once
#include <memory>
#include "operator.h"

namespace ToyDBMS {
	class AbstractNLJoin : public Operator {
		protected:
			std::unique_ptr<Operator> left;
			std::unique_ptr<Operator> right;


			std::vector<Row>::iterator right_ptr;

		private:
			std::shared_ptr<Header> header_ptr;
			Row current_left;

			std::vector<Row> cachedRight;

			Header construct_header(const Header &h1, const Header &h2){
				Header res {h1};
				res.insert(res.end(), h2.begin(), h2.end());
				return res;
			}

		public:
			AbstractNLJoin(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right)
				: left(std::move(left)), right(std::move(right)),
				  header_ptr(std::make_shared<Header>(
					construct_header(this->left->header(), this->right->header()))
				  ) {
				Row current_right = this->right->next();
				while (current_right) {
					cachedRight.push_back(current_right);
					current_right = this->right->next();
				}

				right_ptr = cachedRight.begin();
			}

			const Header &header() override { return *header_ptr; }
			Row next() override;
			void reset() override;

		protected:
			virtual bool isAcceptable(const Row &leftRow, const Row &rightRow) = 0;
	};

	class NLJoin : public AbstractNLJoin {
		std::string left_attr, right_attr;
		Header::size_type left_index, right_index;

		public:
			NLJoin(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right,
				   std::string left_attr, std::string right_attr)
				: AbstractNLJoin(std::move(left), std::move(right)),
				  left_attr(left_attr), right_attr(right_attr),
				  left_index(this->left->header().index(left_attr)),
				  right_index(this->right->header().index(right_attr)) {}

		protected:
			bool isAcceptable(const Row &leftRow, const Row &rightRow) override;
	};

	class CrossJoin : public AbstractNLJoin {
	    Row current_left;

		public:
			CrossJoin(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right)
				: AbstractNLJoin(std::move(left), std::move(right)) {}

		protected:
			bool isAcceptable(const Row &leftRow, const Row &rightRow) override;
	};
}
