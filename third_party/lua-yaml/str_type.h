#include <string.h>

enum yaml_type{YAML_NO_MATCH = 0, YAML_FALSE, YAML_TRUE, YAML_NULL};

/*
 * This implementation uses a subset of yaml 1.1 keywords
 * which represents boolean.
 */
static yaml_type
yaml_get_bool(const char *str, const size_t len){
	if (len > 5)
		return YAML_NO_MATCH;
	if (strcmp(str, "false") == 0 ||
	    strcmp(str, "no") == 0)
		return YAML_FALSE;
	if (strcmp(str, "true") == 0 ||
	    strcmp(str, "yes") == 0)
		return YAML_TRUE;
	return YAML_NO_MATCH;
}

static yaml_type
yaml_get_null(const char *str, const size_t len){
	if (len == 0 || (len == 1 && str[0] == '~'))
		return YAML_NULL;
	if (len == 4) {
		if ((strcmp(str, "null") == 0)
		    || (strcmp(str, "Null") == 0)
		    || (strcmp(str, "NULL") == 0))
			return YAML_NULL;
	}
	return YAML_NO_MATCH;
}
