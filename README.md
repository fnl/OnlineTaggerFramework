# Online Tagger Framework

A CREOLE plugin for [GATE](https://gate.ac.uk) that provides a processing resource similar to the built-in Generic Tagger, but does not do any disk I/O for inter-process communication nor does it launch more than one single, continuous tagger process.
However, this puts some restrictions on the kinds of taggers you can use:

1. The tagger must support **streaming I/O**.
That is, the tagger must be able to read from some "input stream", such as UNIX' `STDIN`, and write to some "output stream", commonly UNIX' `STDOUT`.
Another way of putting this is that your tagger should be able to handle UNIX' piped command syntax, something like this: `cat plain_text.txt | some_tagger > tagged_text.txt`.

2. The tagger must work with POSIX' classical line-based interface.
That is, the tagger must take one continuous block of text as *input*, **terminated with a newline** character.
For example, it should take one token, sentences or block of text as input (not containing any newlines), and, once it receives a newline character, start tagging that input.

3. The tagger must produce **one annotation per line** as *output*, and those annotations must be **in the same order** as the (input) text spans which they annotate.
Those annotations commonly are expected to be in the OTPL (one token per line) format.
For example, the line `Nouns noun NN B-NP O` might annotate the token "Noun" (verbatim, as found in the input text) with the lemma "noun", the PoS-tag "NN", the BIO-chunk "B-NP" and the BIO-NER-tag "O" (outside any entity mention).
The exact structure and contents of that line can be configured, just as with the built-in GenericTagger.

Please clone this plugin from GitHub (`git clone https://github.com/fnl/OnlineTaggerFramework`) into your local CREOLE plugin directory.
Then you can load it with the CREOLE Plugin Manager and once you create a new `GenericOnlineTagger` Processing Resource, you will be asked to supply the basic configuration data needed to start the tagger sub-process: the full path to the tagger binary, the directory to run in (if any), and the runtime flags and arguments (if any).
