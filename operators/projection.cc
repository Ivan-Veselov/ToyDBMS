#include <algorithm>
#include <iterator>
#include "projection.h"

namespace ToyDBMS {
	Row Projection::next(){
		Row row = child->next();
		if(!row) return {};

		std::vector<Value> values;
		values.reserve(header_ptr->size());

		for (const std::string &attributeName : *header_ptr) {
			values.push_back(row[row.header->index(attributeName)]);
		}

		return {header_ptr, std::move(values)};
	}
}
