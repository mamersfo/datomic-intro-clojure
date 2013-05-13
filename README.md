Introduction to Datomic workshop, Clojure version

Might be a good idea to do this *before* the workshop takes place:

Clone this repository

Install [Leiningen](https://github.com/technomancy/leiningen)

Perform

	lein deps

And run the tests (exercises):

	lein test

The time-travel test will fail, because:

	(not (= "Cristiano Ronaldo" "Samuel Eto'o"))

But that's allright, you'll fix that during the workshop.