#pragma once
#include <memory>
#include "../parser/query.h"
#include "../operators/operator.h"

namespace ToyDBMS {
	template<typename T>
	class ValueOrInf {
		private:
			enum Type { MINUS_INF, VALUE, PLUS_INF };

			T value;
			Type type;

		ValueOrInf(const T &value, Type type)
		: value(value), type(type) { }

		public:
			ValueOrInf& createValue(const T &value) {
				return ValueOrInf(value, VALUE);
			}

			ValueOrInf& plusInf() {
				return ValueOrInf(T(), PLUS_INF);
			}

			ValueOrInf& minusInf() {
				return ValueOrInf(T(), MINUS_INF);
			}

			bool operator < (const ValueOrInf &other) {
				if (type == MINUS_INF) {
					return other.type != MINUS_INF;
				}

				if (type == PLUS_INF) {
					return false;
				}

				if (other.type == PLUS_INF) {
					return true;
				}

				if (other.type == MINUS_INF) {
					return false;
				}

				return value < other.value;
			}
	};

	template<typename T>
	class ValuesSegment {
		private:
			ValueOrInf<T> minimumValue;
			ValueOrInf<T> maximumValue;

		public:
			ValuesSegment()
			: minimumValue(ValueOrInf<T>::minusInf()), maximumValue(ValueOrInf<T>::plusInf()) {
			}

			void addUpperBound(const T &value) {
				maximumValue = min(maximumValue, value);
			}

			void addLowerBound(const T &value) {
				minimumValue = max(minimumValue, value);
			}

			bool isEmpty() {
				return minimumValue > maximumValue;
			}
	};
}
