#pragma once
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <functional>

namespace ToyDBMS {

struct Value {
    enum class Type { INT, STR };
    Type type;
    int intval;
    std::string strval;

    Value(int i): type(Type::INT), intval(i) {}
    Value(std::string s): type(Type::STR), strval(s) {}
    Value(std::string v, Type type): type(type) {
        if(type == Type::INT) intval = std::stoi(v);
        else strval = v;
    }

    bool operator==(const Value &other) const {
        if(type != other.type) return false;
        switch(type){
        case Type::INT:
            return intval == other.intval;
        case Type::STR:
            return strval == other.strval;
        default: throw std::runtime_error("unknown value type");
        }
    }

    bool operator!=(const Value &other) const {
        return !(*this == other);
    }

    bool operator<(const Value &other) const {
        if(type != other.type)
            throw std::runtime_error("can't compare values of two different types");
        switch(type){
        case Type::INT:
            return intval < other.intval;
        case Type::STR:
            return strval < other.strval;
        default: throw std::runtime_error("unknown value type");
        }
    }

    bool operator>(const Value &other) const {
        if(type != other.type)
            throw std::runtime_error("can't compare values of two different types");
        switch(type){
        case Type::INT:
            return intval > other.intval;
        case Type::STR:
            return strval > other.strval;
        default: throw std::runtime_error("unknown value type");
        }
    }

    bool operator <= (const Value &other) const {
    	return *this == other || *this < other;
    }

    bool operator >= (const Value &other) const {
		return other <= *this;
	}
};

struct Header : public std::vector<std::string> {
    using vector::vector;

    size_type index(std::string attr) const {
        for(size_type i = 0; i < size(); i++)
            if((*this)[i] == attr) return i;
        throw std::runtime_error("unknown attribute " + attr);
    }
};

struct Row {
    using size_type = std::vector<Value>::size_type;
    std::shared_ptr<Header> header;
    std::vector<Value> values;

    Row(){}

    Row(const std::shared_ptr<Header> &header, std::vector<Value> &&values)
        : header(header), values(std::move(values)) {}

    Row(const Row &other) = default;
    Row(Row &&other) = default;
    Row &operator=(const Row &other) = default;
    Row &operator=(Row &&other) = default;

    Value &operator[](size_type i){
        return values[i];
    }

    Value &operator[](std::string attr){
        return values[header->index(attr)];
    }

    const Value &operator[](size_type i) const {
        return values[i];
    }

    const Value &operator[](std::string attr) const {
        return values[header->index(attr)];
    }

    bool operator==(const Row &other) const {
    	for (size_t i = 0; i < values.size(); i++) {
    		if (values[i] != other.values[i]) {
    			return false;
    		}
    	}

    	return true;
    }

    size_type size() const { return values.size(); }

    operator bool(){ return !values.empty(); }
};

inline std::ostream &operator<<(std::ostream &os, const Header &header){
    if(header.empty()) return os;
    for(Row::size_type i = 0; i < header.size() - 1; i++)
        os << header[i] << '\t';
    os << header.back();
    return os;
}

inline std::ostream &operator<<(std::ostream &os, const Value &val){
    switch(val.type){
    case Value::Type::INT:
        return os << val.intval;
    case Value::Type::STR:
        return os << val.strval;
    default: throw std::runtime_error("unknown value type");
    }
}

inline std::ostream &operator<<(std::ostream &os, const Row &row){
    for(Row::size_type i = 0; i < row.size() - 1; i++)
        os << row.values[i] << '\t';
    os << row.values.back();
    return os;
}

}

namespace std {
template <>
struct hash<ToyDBMS::Value> {
	size_t operator()(const ToyDBMS::Value &v) const {
		switch (v.type) {
			case ToyDBMS::Value::Type::INT: {
				std::hash<int> hasher;
				return hasher(v.intval);
			}

			case ToyDBMS::Value::Type::STR: {
				std::hash<std::string> hasher;
				return hasher(v.strval);
			}

			default:
				throw std::runtime_error("Unexpected Value type");
		}
	}
};

}

namespace ToyDBMS {
struct RowHasher {
    size_t operator()(const Row &r) const {
        std::hash<Value> hasher;

        size_t seed = 0;
        for (const Value &v : r.values) {
            seed ^= hasher(v) + 0x9e3779b9 + (seed<<6) + (seed>>2);
        }

        return seed;
    }
};
}
