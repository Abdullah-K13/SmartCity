
def groupAnagrams(strs: list[str]) -> list[list[str]]:
        final = []
        sub = []
        countS,countT = {},{}
        for i in range(len(strs)):
            #sub.append(strs[i])
            # sub.append(strs[i])

            for j in range(i+1, len(strs)):
                if len(strs[i]) == len(strs[j]):                     
                     if ''.join(sorted(strs[i])) == ''.join(sorted(strs[j])):
                          print("these twoe are anagrams")
                          print(strs[i])
                          print(strs[j])
                          sub.append(strs[j])
                          sub.append(strs[i])

                final.append(sub)
                sub.clear()
        return final

strs=["act","pots","tops","cat","stop","hat"]
final = groupAnagrams(strs)
print(final)

# str = 'sabo'
# print(''.join(sorted(str)))

