/*    Copyright 2015 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include <algorithm>
#include <iostream>
#include <iomanip>

#include "mongo/db/query/shapeanalyzer/shapeanalyzer.h"

namespace mongo {

    ShapeAnalysisResult::ShapeAnalysisResult()
        : timesSeen(0) {
    }

    void ShapeAnalysisResult::computeStats() {
        std::sort(millis.begin(), millis.end(), std::greater<int>());
        this->minMillis = millis.front();
        this->maxMillis = millis.back();

        size_t sum = 0;
        for (size_t n : millis) {
            sum += n;
        }
        this->meanMillis = (static_cast<double>(sum) / static_cast<double>(millis.size()));
    }

    void ShapeAnalysisResult::report() const {
        // TODO
        /*
        std::cout << std::left << std::setfill(" ")
                  << std::setw(15) << this->ns << "\t"
                  << std::setw(15) << this->predicate << "\t"
                  << std::setw(15) << this->projection << "\t"
                  << std::setw(15) << this->sort << "\t"
                  << std::setw(6)  << this->timesSeen << std::end;
        */
    }

    void ShapeAnalyzer::add(const QueryLogParser& logParser) {
        ShapeAnalysisKey analysisKey;
        const CanonicalQuery& query = logParser.getCanonicalQuery();
        analysisKey.cacheKey = query.getPlanCacheKey();
        analysisKey.ns = logParser.getNS();

        ShapeAnalysisResult& result = _shapes[analysisKey];

        if (result.timesSeen == 0U) {
            result.ns = analysisKey.ns;
            result.predicate = logParser.getPredicate();
            result.projection = logParser.getProjection();
            result.sort = logParser.getSort();
        }

        result.timesSeen++;
        result.millis.push_back(logParser.getMillis());
    }

    struct ShapeAnalysisResultComparator {
        bool operator()(const ShapeAnalysisResult& left, const ShapeAnalysisResult& right) const {
            return left.meanMillis < right.meanMillis;
        }
    };

    void ShapeAnalyzer::computeStats() {
        for (auto i = _shapes.begin(); i != _shapes.end(); ++i) {
            ShapeAnalysisResult& shapeResult = i->second;
            shapeResult.computeStats();
            _sortedShapes.push_back(shapeResult);
        }

        std::sort(_sortedShapes.begin(), _sortedShapes.end(), ShapeAnalysisResultComparator());
    }

    void ShapeAnalyzer::report() const {
        std::cout << std::left << std::setfill(" ")
                  << std::setw(15) << "namespace" << "\t"
                  << std::setw(15) << "predicate_shape" << "\t"
                  << std::setw(15) << "projection_shape" << "\t"
                  << std::setw(15) << "sort_shape" << "\t"
                  << std::setw(6)  << "count" << std::endl;

        for (ShapeAnalysisResult shape : _sortedShapes) {
            shape.report();
        }
    }

} // namespace mongo
