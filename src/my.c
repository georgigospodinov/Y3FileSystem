#include <stdio.h>

#define CHECKED_CALL(func, ...) rc = (*(func))(__VA_ARGS__); if (rc) return rc


int mcmp(int a, int b) {
    if (a < b) return -1;
    else if (a > b) return 1;
    else return 0;
}

int mul(int a, int b) {
    printf("%d\n", a * b);
    return 0;
}

int add(int a, int b, int c) {
    printf("%d\n", a + b+c);
    return 0;
}

int main() {

    int rc;
    CALL(&mul, 10, 11);
    CALL(&add, 10, 11, 14);

}
