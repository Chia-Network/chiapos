// Copyright 2022 Chia Network Inc

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_SERIALIZE_HPP_
#define SRC_SERIALIZE_HPP_

#include <stdexcept>
#include <string>
#include <vector>

template<typename Type> class Serializable{
public:
    static void SerializeImpl(const Type& in, std::vector<uint8_t>& out)
    {
        size_t nSize = sizeof(in);
        out.reserve(out.size() + nSize);
        for (size_t i = 0; i < nSize; ++i) {
            out.push_back(*((uint8_t*)&in + i));
        }
    }
    static size_t DeserializeImpl(const std::vector<uint8_t>& in, Type& out, size_t position)
    {
        size_t size = sizeof(out);
        if (position + size > in.size()) {
            throw std::invalid_argument("DeserializeImpl: Trying to read out of bounds.");
        }
        for (size_t i = 0; i < size; ++i) {
            *((uint8_t*)&out + i) = in[position + i];
        }
        return size;
    }
};

template<typename TypeIn, typename TypeOut>
void Serialize(const TypeIn& in, TypeOut& out)
{
    Serializable<TypeIn>::SerializeImpl(in, out);
}

template<typename TypeIn, typename TypeOut>
size_t Deserialize(const TypeIn& in, TypeOut& out, const size_t position)
{
    return Serializable<TypeOut>::DeserializeImpl(in, out, position);
}

template<typename TypeIn, typename TypeOut>
void SerializeContainer(const TypeIn& in, TypeOut& out)
{
    Serialize(in.size(), out);
    for (auto& entry : in) {
        Serialize(entry, out);
    }
}

template<typename TypeIn, typename TypeOut>
size_t DeserializeContainer(const TypeIn& in, TypeOut& out, const size_t position)
{
    size_t elements;
    size_t size = Deserialize(in, elements, position);
    if (elements == 0) {
        return size;
    }
    out.clear();
    out.resize(elements);
    for (size_t i = 0; i < elements; ++i) {
        size += Deserialize(in, out[i], position + size);
    }
    return size;
}

template<typename Type>
class Serializable<std::vector<Type>>{
public:
    static void SerializeImpl(const std::vector<Type>& in, std::vector<uint8_t>& out)
    {
        SerializeContainer(in, out);
    }
    static size_t DeserializeImpl(const std::vector<uint8_t>& in, std::vector<Type>& out, const size_t position)
    {
        return DeserializeContainer(in, out, position);
    }
};

template<>
class Serializable<std::string>{
public:
    static void SerializeImpl(const std::string& in, std::vector<uint8_t>& out)
    {
        SerializeContainer(in, out);
    }
    static size_t DeserializeImpl(const std::vector<uint8_t>& in, std::string& out, const size_t position)
    {
        return DeserializeContainer(in, out, position);
    }
};

class Serializer
{
    std::vector<uint8_t> data;
public:
    template <typename InputType>
    friend Serializer& operator <<(Serializer& serializer, const InputType& value) {
        Serialize(value, serializer.data);
        return serializer;
    }
    std::vector<uint8_t>& Data()
    {
        return data;
    }
    void Reset()
    {
        data.clear();
    }
};


class Deserializer
{
    size_t position{0};
    const std::vector<uint8_t>& data;
public:
    explicit Deserializer(const std::vector<uint8_t>& data) : data(data) {}
    void Reset()
    {
        position = 0;
    }
    template <typename OutputType>
    friend Deserializer& operator>>(Deserializer& deserializer, OutputType& output) {
        deserializer.position += Deserialize(deserializer.data,
                                             output,
                                             deserializer.position);
        return deserializer;
    }
    bool End() const
    {
        return position == data.size();
    }
};

#endif  // SRC_SERIALIZE_HPP_
