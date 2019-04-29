package util

import "sort"

func SliceDiff(ref, new []string) (added, missing []string) {
	sort.Strings(ref)
	sort.Strings(new)
	added = []string{}
	missing = []string{}
	if ref == nil {
		return new, []string{}
	}
	if new == nil {
		return []string{}, ref
	}

	// Single pass logic to avoid too big O for big services
	lenRef := len(ref)
	lenNew := len(new)
	maxLen := lenRef
	if lenNew > maxLen {
		maxLen = lenNew
	}

	refI := 0
	newI := 0
	// Exhaust shared lenght elements
	for refI < lenRef && newI < lenNew {
		refS := ref[refI]
		newS := new[newI]
		if refS == newS {
			refI++
			newI++
			continue
		}
		if refS < newS {
			missing = append(missing, refS)
			refI++
		} else {
			added = append(added, newS)
			newI++
		}
	}
	// Check if ref is still containing stuff; adding them to missing elems
	for refI < lenRef {
		missing = append(missing, ref[refI])
		refI++
	}
	// Check if new is still containing stuff; adding them to new elements
	for newI < lenRef {
		added = append(added, new[newI])
		newI++
	}
	return
}

func SliceEquals(one, two []string) bool {
	if (one == nil && two != nil) || (one != nil && two == nil) {
		return false
	}
	if len(one) != len(two) {
		return false
	}
	for i := range one {
		if one[i] != two[i] {
			return false
		}
	}
	return true
}
