#include "sorter.h"

row * mergesort(row * head) {
	if (head == NULL) {
		return NULL;
	}

	if (head->next == NULL) {
		return head;
	}

	row * slow;
	row * fast;
	row * prevSlow;

	slow = head;
	fast = head;

	while (fast != NULL) {
		prevSlow = slow;
		slow = slow->next;
		fast = fast->next;

		if (fast != NULL) {
			fast = fast->next;
		}
	}

	prevSlow->next = NULL;

	row * firstHalf;
	row * secondHalf;
	row * whole;

	firstHalf = mergesort(head);
	secondHalf = mergesort(slow);
	whole = merge(firstHalf, secondHalf);
	
	return whole;
}

row * merge(row * first, row * second) {
	row * whole;
	row * fakeHead;
	whole = createRow();
	fakeHead = whole;

	while (first != NULL || second != NULL) {
		if (first != NULL && second != NULL) {
			int res;
			res = strcmp(first->toCompare, second->toCompare);

			int firstAtoI;
			int secondAtoI;
			firstAtoI = atoi(first->toCompare);
			secondAtoI = atoi(second->toCompare);
			
			if (firstAtoI != 0 && secondAtoI != 0) {
				res = firstAtoI - secondAtoI;
			}

			if (res < 0) {
				whole->next = first;
				whole = whole->next;
				first = first->next;
			}
			else if (res == 0) {
				if (first->numRow < second->numRow) {
					whole->next = first;
					whole = whole->next;
					first = first->next;
				} else {
					whole->next = second;
					whole = whole->next;
					second = second->next;
				}
			}
			else if (res > 0) {
				whole->next = second;
				whole = whole->next;
				second = second->next;
			}
		} 
		else if (first != NULL && second == NULL) {
			whole->next = first;
			whole = whole->next;
			first = first->next;
		}
		else if (first == NULL && second != NULL) {
			whole->next = second;
			whole = whole->next;
			second = second->next;
		}
	}

	return fakeHead->next;
}
