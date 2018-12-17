#include <algorithm>
#include <iterator>
#include "unique.h"

namespace ToyDBMS {
	Row Unique::next() {
		while (true) {
			Row r = child->next();
			if (!r) {
				return {};
			}

			if (hashTable.find(r) == hashTable.end()) {
				hashTable.insert(r);
				return r;
			}
		}
	}
}
