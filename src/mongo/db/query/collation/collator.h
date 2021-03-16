/**
 *    Copyright (C) 2021-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#pragma once

#include "mongo/bson/bsonobj_comparator.h"
#include "mongo/db/exec/document_value/document_comparator.h"
#include "mongo/db/exec/document_value/value_comparator.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/query/collation/collator_interface.h"

namespace mongo {

/**
 * Provides a set of comparison semantics, including unicode collation support for locale-aware
 * string comparison semantics.
 */
class Collator {
public:
    using ComparisonRules = BSONObj::ComparatorInterface::ComparisonRules;
    using ComparisonRulesSet = BSONObj::ComparatorInterface::ComparisonRulesSet;

    /**
     * Constructs a default 'Collator'. This will use simple binary comparison for strings, and will
     * consider field names and field order of objects significant.
     */
    Collator() = default;

    /**
     * Implicit conversion from 'CollatorInterface'.
     *
     * TODO: This is a crutch for the POC. It would be best to remove.
     */
    Collator(std::unique_ptr<CollatorInterface> unicodeCollator)
        : Collator(std::move(unicodeCollator), 0) {}

    /**
     * Constructs a 'Collator' based on the applications specification of a collation.
     */
    Collator(BSONObj collationSpec, const CollatorFactoryInterface& unicodeCollatorFactory);

    Collator(std::unique_ptr<CollatorInterface> unicodeCollator, ComparisonRulesSet rulesSet)
        : _rulesSet(rulesSet), _unicodeCollator(std::move(unicodeCollator)) {
        initComparators();
    }

    const DocumentComparator& getDocumentComparator() const {
        return _documentComparator;
    }

    const ValueComparator& getValueComparator() const {
        return _valueComparator;
    }

    const CollatorInterface* getUnicodeCollator() const {
        return _unicodeCollator.get();
    }

    ComparisonRulesSet getComparisonRulesSet() const {
        return _rulesSet;
    }

    // TODO: Can this be done away with?
    void setUnicodeCollator(std::unique_ptr<CollatorInterface> unicodeCollator) {
        _unicodeCollator = std::move(unicodeCollator);
        initComparators();
    }

    // TODO: How necessary is this, really?
    // TODO: Needs to incorporate 'ignoreFieldOrder'.
    BSONObj toBson() const {
        return _unicodeCollator ? _unicodeCollator->getSpec().toBSON() : CollationSpec::kSimpleSpec;
    }

    Collator clone() const;

private:
    void initComparators() {
        // TODO: Need to actually plumb rules set through.
        _documentComparator = DocumentComparator(_unicodeCollator.get());
        _valueComparator = ValueComparator(_unicodeCollator.get());
    }

    ComparisonRulesSet _rulesSet = 0;

    // Used for string comparisons, or null if strings should use a simple binary comparison.
    std::unique_ptr<CollatorInterface> _unicodeCollator;

    // Used for all comparisons of Document/Value during execution of the aggregation operation.
    // Must not be changed after parsing a Pipeline with this ExpressionContext.
    DocumentComparator _documentComparator;
    ValueComparator _valueComparator;
};

}  // namespace mongo
