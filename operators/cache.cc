#include <algorithm>
#include <iterator>
#include "cache.h"

namespace ToyDBMS {
	Row Cache::next() {
		if (iterator == cached.end()){
			return {};
		}

		Row result = *iterator;
		iterator++;

		return result;
	}

	void Cache::reset() {
		iterator = cached.begin();
	}
}
