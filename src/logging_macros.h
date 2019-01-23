#ifndef PROJECT_LOGGING_MACROS_H
#define PROJECT_LOGGING_MACROS_H

// Each type of logging is given a value that is a power of 2.
#define FUNC_ENTRY      (1<<0)
#define ERROR           (1<<1)
#define CLARIFICATION   (1<<2)
#define FCB             (1<<3)
#define META             (1<<4)
#define GENERAL         (1<<10)

/* The types of logging that should be triggered are ORed together.
 * Each type is kept at a separate line for easy commenting out.
 */
const int LOG =
        FUNC_ENTRY |
        ERROR |
        FCB |
        META |
        CLARIFICATION |
        GENERAL |
        0;

const int FORCE_LOG = 0;


// The logging macros check if something should be logged.
#define LOG_FUNC if ((iLog || FORCE_LOG) && (LOG & FUNC_ENTRY)) write_log
#define LOG_ERR if ((iLog || FORCE_LOG) && (LOG & ERROR)) write_log
#define LOG_CLARIFY if ((iLog || FORCE_LOG) && (LOG & CLARIFICATION)) write_log
#define LOG_FCB if ((iLog || FORCE_LOG) && (LOG & FCB)) print_fcb
#define LOG_META if ((iLog || FORCE_LOG) && (LOG & META)) print_meta
#define LOG_GENERAL if ((iLog || FORCE_LOG) && (LOG & GENERAL)) write_log


#define TEST_CONDITION(condition, message, error)\
    if (condition) {\
        LOG_ERR("  !!!");\
        LOG_ERR(message);\
        LOG_ERR("\n");\
        return error;\
    }

/**
 * Calls the given function with the given arguments.
 * If it returns a non-zero value, forces a return with that value.
 * This is used when the error message from the function 'func' is to be shown
 *                   but the function with this macro also needs to exit.
 */
#define CHECKED_CALL(func, ...) rc = func(__VA_ARGS__); if (rc) return rc

#endif //PROJECT_LOGGING_MACROS_H
