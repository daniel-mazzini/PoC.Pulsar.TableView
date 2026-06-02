namespace PoC.Pulsar.TableView.Infrastructure.Store.Extensions;
public static class StringExtensions
{
    public static string ToKebabCaseOptimized(this string str)
    {
        if (string.IsNullOrEmpty(str))
            return str;
        // 1. Calcule how many hyphens we need to add.
        // We start at index 1 because if the first letter is uppercase, it doesn't get a hyphen.
        int additionalHyphens = 0;
        for (int i = 1; i < str.Length; i++)
        {
            if (char.IsUpper(str[i]))
            {
                additionalHyphens++;
            }
        }

        // if there are no uppercase letters after the first character, we can just return the lowercase version of the string
        if (additionalHyphens == 0)
            return str.ToLowerInvariant();

        // 2. Calculate the exact final size
        int newLength = str.Length + additionalHyphens;

        // 3. string.Create preserve memory and gives us a Span<char> to fill it
        return string.Create(newLength, str, (span, state) =>
        {
            int spanIndex = 0;
            for (int i = 0; i < state.Length; i++)
            {
                char c = state[i];
                if (char.IsUpper(c))
                {
                    // Add hyphen before uppercase letters (except the first character)
                    if (i > 0)
                    {
                        span[spanIndex++] = '-';
                    }
                    // Convert uppercase to lowercase and add to the span
                    span[spanIndex++] = char.ToLowerInvariant(c);
                }
                else
                {
                    // Copiar el caracter original si ya es minúscula
                    span[spanIndex++] = c;
                }
            }
        });
    }
}
