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
				bool hasChanged = false;
				for (int i = 0; i < orderedAttributes.size(); ++i) {
					Value currentValue = r[orderedAttributes[i]];

					if (attributeValue.size() < orderedAttributes.size()) {
						attributeValue.push_back(currentValue);
					} else {
						if (attributeValue[i] != currentValue) {
							attributeValue[i] = currentValue;
							hasChanged = true;
						}
					}
				}

				if (hasChanged) {
					hashTable.clear();
				}
			}

			std::vector<Value> compressed = compress(r);
			if (hashTable.find(compressed) == hashTable.end()) {
				hashTable.insert(compressed);
				return r;
			}
		}
	}

	std::vector<Value> OptimizedUnique::compress(const Row &row) {
		std::vector<Value> result;
		for (int index : indicesOfNotOrdered) {
			result.push_back(row.values[index]);
		}

		return std::move(result);
	}
}
