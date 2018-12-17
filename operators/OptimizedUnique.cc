#include <algorithm>
#include <iterator>
#include "OptimizedUnique.h"

namespace ToyDBMS {
	Row OptimizedUnique::next() {
		while (true) {
			Row r = child->next();
			if (!r) {
				return {};
			}

			if (hashTable.size() > 0) {
				if ((*hashTable.begin())[orderedAttribute] != r[orderedAttribute]) {
					hashTable.clear();
				}
			}

			if (hashTable.find(r) == hashTable.end()) {
				hashTable.insert(r);
				return r;
			}
		}
	}
}
