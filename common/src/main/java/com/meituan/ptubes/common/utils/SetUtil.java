package com.meituan.ptubes.common.utils;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;

public class SetUtil {

    public static <T> boolean isDifferent(Set<T> setA, Set<T> setB) {
        if (setA.size() != setB.size()) {
            return true;
        } else {
            for (T a : setA) {
                if (setB.contains(a)) {
                    continue;
                } else {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param setA
     * @param setB
     * @param <T>
     * @return
     * input:
     * --------------------------------
     * |         A         |
     * --------------------------------
     *            |         B         |
     * --------------------------------
     * ans: left = A-B, right = B-A
     * --------------------------------
     * |   left   |        |   right  |
     * --------------------------------
     */
    public static <T> Pair<Set<T>, Set<T>> minus(Set<T> setA, Set<T> setB) {
        Set<T> setAB = new HashSet<>();
        setAB.addAll(setA);
        setAB.addAll(setB);

        Set<T> exSetA = new HashSet<>(setAB);
        Set<T> exSetB = new HashSet<>(setAB);
        exSetA.removeAll(setB);
        exSetB.removeAll(setA);
        return Pair.of(exSetA, exSetB);
    }

    public static <T> Set<T> intersect(Set<T> setA, Set<T> setB) {
        Set<T> ans = new HashSet<>(setA);
        ans.retainAll(setB);
        return ans;
    }
}
