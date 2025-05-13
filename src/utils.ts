export const sleep = (ms: number) =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

export const createSubject = (name: string) => {
  return `${name}.*`;
};
