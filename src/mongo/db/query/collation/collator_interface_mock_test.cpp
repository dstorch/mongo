/**
 *    Copyright (C) 2016 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/query/collation/collator_interface_mock.h"

#include "mongo/unittest/unittest.h"

namespace {

using namespace mongo;

TEST(CollatorInterfaceMockSelfTest, MocksOfSameTypeAreEqual) {
    CollatorInterfaceMock reverseMock1(CollatorInterfaceMock::MockType::kReverseString);
    CollatorInterfaceMock reverseMock2(CollatorInterfaceMock::MockType::kReverseString);
    ASSERT(reverseMock1 == reverseMock2);

    CollatorInterfaceMock alwaysEqualMock1(CollatorInterfaceMock::MockType::kAlwaysEqual);
    CollatorInterfaceMock alwaysEqualMock2(CollatorInterfaceMock::MockType::kAlwaysEqual);
    ASSERT(alwaysEqualMock1 == alwaysEqualMock2);
}

TEST(CollatorInterfaceMockSelfTest, MocksOfDifferentTypesAreNotEqual) {
    CollatorInterfaceMock reverseMock(CollatorInterfaceMock::MockType::kReverseString);
    CollatorInterfaceMock alwaysEqualMock(CollatorInterfaceMock::MockType::kAlwaysEqual);
    ASSERT(reverseMock != alwaysEqualMock);
}

TEST(CollatorInterfaceMockSelfTest, ReverseMockComparesInReverse) {
    CollatorInterfaceMock reverseMock(CollatorInterfaceMock::MockType::kReverseString);
    ASSERT_EQ(reverseMock.compare("abc", "abc"), 0);
    ASSERT_GT(reverseMock.compare("abc", "cba"), 0);
    ASSERT_LT(reverseMock.compare("cba", "abc"), 0);
}

TEST(CollatorInterfaceMockSelfTest, ReverseMockComparisonKeysCompareInReverse) {
    CollatorInterfaceMock reverseMock(CollatorInterfaceMock::MockType::kReverseString);
    auto keyABC = reverseMock.getComparisonKey("abc");
    auto keyCBA = reverseMock.getComparisonKey("cba");
    ASSERT_EQ(keyABC.compare(keyABC), 0);
    ASSERT_GT(keyABC.compare(keyCBA), 0);
    ASSERT_LT(keyCBA.compare(keyABC), 0);
}

TEST(CollatorInterfaceMockSelfTest, AlwaysEqualMockAlwaysComparesEqual) {
    CollatorInterfaceMock alwaysEqualMock(CollatorInterfaceMock::MockType::kAlwaysEqual);
    ASSERT_EQ(alwaysEqualMock.compare("abc", "efg"), 0);
    ASSERT_EQ(alwaysEqualMock.compare("efg", "abc"), 0);
    ASSERT_EQ(alwaysEqualMock.compare("abc", "abc"), 0);
}

TEST(CollatorInterfaceMockSelfTest, AlwaysEqualMockComparisonKeysAlwaysCompareEqual) {
    CollatorInterfaceMock alwaysEqualMock(CollatorInterfaceMock::MockType::kAlwaysEqual);
    auto keyABC = alwaysEqualMock.getComparisonKey("abc");
    auto keyEFG = alwaysEqualMock.getComparisonKey("efg");
    ASSERT_EQ(keyABC.compare(keyEFG), 0);
    ASSERT_EQ(keyEFG.compare(keyABC), 0);
    ASSERT_EQ(keyABC.compare(keyABC), 0);
}
};
