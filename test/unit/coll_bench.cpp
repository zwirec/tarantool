#include <iostream>
#include <string.h>
#include <assert.h>
#include <diag.h>
#include <fiber.h>
#include <memory.h>
#include <chrono>
#include <fstream>
#include "coll_def.h"
#include "coll.h"
#include "unit.h"
#include "third_party/PMurHash.h"

class
Timer
{
public:
	Timer(): start_(std::chrono::high_resolution_clock::now())
	{
	}

	~Timer()
	{
		const auto finish = std::chrono::high_resolution_clock::now();
		std::cout << std::chrono::duration_cast<std::chrono::microseconds>(finish - start_).count() << " us" << std::endl;
	}

private:
	const std::chrono::high_resolution_clock::time_point start_;
};

void
comparing(char **strings, size_t size, struct coll *coll)
{
	Timer t;
	for (size_t i = 0; i < size; ++i) {
		for (size_t j = 0; j < size; ++j) {
			char *a = strings[i];
			char *b = strings[j];
			coll->cmp(a, strlen(a), b, strlen(b), coll);
		}
	}
}

void
reading(char **strings, size_t size, const char *path)
{
	std::ifstream read(path);
	std::string str;
	size_t i;
	for (i = 0; i < size && read >> str; ++i) {
		const char *cstr = str.c_str();
		size_t len = strlen(cstr) + 1;
		char *tmp = (char *)malloc(len);
		memcpy(tmp, cstr, len);
		strings[i] = tmp;
	}
	assert(i == size);
}

void
bench(size_t size, const char *text, const char *locale)
{
	char **strings = (char **)malloc(size * sizeof(char *));
	reading(strings, size, text);// "./rus.txt");

	struct coll_def def;
	memset(&def, 0, sizeof(def));
	snprintf(def.locale, sizeof(def.locale), "%s", locale);
	def.type = COLL_TYPE_ICU;
	struct coll *coll;

	std::cout << "Size: " << size << std::endl;

	def.icu.strength = COLL_ICU_STRENGTH_IDENTICAL;
	coll = coll_new(&def);
	comparing(strings, size, coll);
	coll_unref(coll);

	def.icu.strength = COLL_ICU_STRENGTH_PRIMARY;
	coll = coll_new(&def);
	comparing(strings, size, coll);
	coll_unref(coll);

	std::cout << "Finished" << std::endl;
	for (size_t i = 0; i < size; ++i)
		free(strings[i]);
}

int
main(int, const char**)
{
	coll_init();
	memory_init();
	fiber_init(fiber_c_invoke);
	std::cout << "Language: Eng" << std::endl;
	bench(10000, "./eng.txt", "en_EN");
	std::cout << "\n" << "Language: Rus" << std::endl;
	bench(10000, "./rus.txt", "ru_RU");
	fiber_free();
	memory_free();
	coll_free();
}
